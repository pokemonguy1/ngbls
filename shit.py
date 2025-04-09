import asyncio
import sys
import logging
import json
import os
import math
from collections import defaultdict
from cryptofeed import FeedHandler
from cryptofeed.callback import TickerCallback
from cryptofeed.defines import TICKER

# Import necessary exchange classes
from cryptofeed.exchanges import (
    Binance, Bitget, Huobi, Gateio, OKX, Kraken, Bitfinex
)

# --- Configuration ---
JSON_FILENAME = "cex_price.json" # Aggregated output file
WRITE_INTERVAL = 2.0

# Define target exchanges and their classes
TARGET_EXCHANGES = {
    "BINANCE": Binance,
    "BITGET": Bitget,
    "HTX": Huobi,      # Cryptofeed uses HTX for Huobi
    "GATEIO": Gateio,
    "OKX": OKX,
    "KRAKEN": Kraken,
    "Bitfinex": Bitfinex,
}

# Map internal names to display names for JSON output
EXCHANGE_NAME_MAP = {
    "BINANCE": "Binance",
    "BITGET": "Bitget",
    "HTX": "Huobi",    # Display name can still be Huobi
    "GATEIO": "Gate.io",
    "OKX": "OKX",
    "KRAKEN": "Kraken",
    "BITFINEX": "Bitfinex",
}

# Define connection limits (adjust if needed, especially for specific exchanges)
SYMBOL_LIMITS = {
    # Add exchange-specific limits here if default is too high
    # e.g., "KUCOIN": 20, # Example if Kucoin was included
}
DEFAULT_SYMBOL_LIMIT = 100 # Default limit per connection if not specified above

# Define target quote assets for filtering
TARGET_QUOTE_ASSETS = {"USDT", "USDC", "USD"}

# --- End Configuration ---

# Configure logging (Keep DEBUG for connection diagnostics)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Filter out noisy logs unless debugging websockets internals
logging.getLogger('websockets').setLevel(logging.INFO)
logging.getLogger('cryptofeed.connection').setLevel(logging.DEBUG) # Keep DEBUG for connection issues

# --- Global State and Lock ---
current_prices = defaultdict(list)
price_data_lock = asyncio.Lock()

# --- Async Functions ---

async def ticker_handler(ticker, receipt_timestamp):
    """Callback function to handle incoming ticker data from ANY exchange."""
    # Log entry confirmation
    # logger.critical(f"!!!!!! TICKER HANDLER ENTERED: {ticker.exchange} / {ticker.symbol} - Bid: {ticker.bid} / Ask: {ticker.ask} !!!!!!")

    internal_exchange_name = ticker.exchange
    display_cex_name = EXCHANGE_NAME_MAP.get(internal_exchange_name, internal_exchange_name) # Fallback to internal name if not mapped

    # Price Extraction: Prioritize 'bid'. Add fallbacks ONLY if necessary for a specific exchange.
    price = ticker.bid

    if price is None:
        # Log if bid is missing. Add exchange-specific fallbacks here if needed.
        # Example for a hypothetical exchange 'XYZ' needing 'last':
        # if internal_exchange_name == "XYZ":
        #     price = ticker.last
        #     if price is not None:
        #         logger.debug(f"[{internal_exchange_name}] Using LAST price ({price}) for {ticker.symbol} as Bid was None.")
        #     else:
        #          logger.debug(f"[{internal_exchange_name}] Skipping {ticker.symbol}: Bid and Last are both None.")
        #          return
        # else: # Default behavior for other exchanges
        logger.debug(f"[{internal_exchange_name}] Skipping {ticker.symbol}: Bid is None.")
        return # Skip if bid is None and no fallback used/succeeded

    # --- Process the price data ---
    symbol_parts = ticker.symbol.split('-')
    if len(symbol_parts) != 2:
        logger.warning(f"[{internal_exchange_name}] Skipping unexpected symbol format: {ticker.symbol}")
        return
    json_symbol_key = f"{symbol_parts[0]}/{symbol_parts[1]}"

    try:
        float_price = float(price)
    except (ValueError, TypeError) as e:
         logger.warning(f"[{internal_exchange_name}] Could not convert price '{price}' to float for {ticker.symbol}. Error: {e}. Skipping.")
         return

    new_entry = { "price": float_price, "network": "CEX", "cex": display_cex_name }

    # logger.debug(f"[{internal_exchange_name}_HANDLER] Acquiring lock for {json_symbol_key} - {display_cex_name} - Price: {float_price}")
    async with price_data_lock:
        # logger.debug(f"[{internal_exchange_name}_HANDLER] Lock acquired. Updating {json_symbol_key} - {display_cex_name}")
        exchange_found = False
        # Use list directly since it's defaultdict(list)
        for entry in current_prices[json_symbol_key]:
            if entry["cex"] == display_cex_name:
                entry["price"] = new_entry["price"]
                exchange_found = True
                break
        if not exchange_found:
            current_prices[json_symbol_key].append(new_entry)


async def periodic_json_writer(filename, interval):
    """Periodically writes the current_prices dictionary to a JSON file."""
    # This function remains the same
    logger.critical(">>> periodic_json_writer task started <<<") # Add this
    temp_filename = filename + ".tmp"; logged_write_error = False
    loop_count = 0
    while True:
        loop_count += 1
        logger.critical(f">>> Writer loop starting iteration {loop_count} <<<") # Add this
        try: await asyncio.sleep(interval); logged_write_error = False
        except asyncio.CancelledError: logger.info("JSON writer task cancelled."); break
        except Exception as e: logger.error(f"Error in JSON writer sleep: {e}"); await asyncio.sleep(interval * 2); continue
        try:
            async with price_data_lock:
                logger.debug(f"Lock acquired. Writing {len(current_prices)} distinct symbols to {filename}...")
                # Count total price entries across all symbols
                total_entries = sum(len(prices) for prices in current_prices.values())
                logger.debug(f"Total price entries being written: {total_entries}")
                data_to_write = dict(current_prices)
            with open(temp_filename, 'w') as file:
                file.write(json.dumps(data_to_write, indent=4))
            os.replace(temp_filename, filename)
            logger.debug(f"Successfully wrote data to {filename}")
        except IOError as e:
            if not logged_write_error: logger.error(f"Error writing JSON {filename}: {e}"); logged_write_error = True
        except Exception as e:
            if not logged_write_error: logger.error(f"Unexpected error during JSON write: {e}", exc_info=True); logged_write_error = True


# --- Synchronous Helper Functions ---

def get_symbols_to_monitor_sync(exchange_class, exchange_name, target_quotes):
    """Fetches all symbols from the exchange and filters for target quote assets."""
    # This function remains the same as the previous Binance-only auto-fetch version
    logger.info(f"[{exchange_name}] Fetching ALL supported symbols via REST API...")
    try:
        exchange_instance = exchange_class()
        all_supported_symbols = exchange_instance.symbols()
        if not all_supported_symbols or not isinstance(all_supported_symbols, (list, set)):
             logger.error(f"[{exchange_name}] Invalid/empty symbol list received. Type: {type(all_supported_symbols)}."); return None
        logger.info(f"[{exchange_name}] Obtained {len(all_supported_symbols)} total symbols.")
    except Exception as e:
        logger.error(f"[{exchange_name}] Error obtaining symbols via REST API: {e}.", exc_info=True); return None

    symbols_to_subscribe = []; filtered_out_count = 0; invalid_format_count = 0
    logger.info(f"[{exchange_name}] Filtering symbols for quote assets: {', '.join(target_quotes)}...")
    for symbol in all_supported_symbols:
        parts = symbol.split('-')
        if len(parts) == 2:
            base_asset, quote_asset = parts
            if quote_asset in target_quotes: symbols_to_subscribe.append(symbol)
            else: filtered_out_count += 1
        else: invalid_format_count += 1; filtered_out_count +=1
    if invalid_format_count > 0: logger.warning(f"[{exchange_name}] Skipped {invalid_format_count} symbols due to unexpected format.")
    if not symbols_to_subscribe: logger.warning(f"[{exchange_name}] No symbols found matching quote assets ({', '.join(target_quotes)}).")
    else:
        logger.info(f"[{exchange_name}] Filtered down to {len(symbols_to_subscribe)} symbols with target quote assets.")
        logger.info(f"[{exchange_name}] ({filtered_out_count} symbols excluded).")
    return symbols_to_subscribe


# --- Main Execution Logic ---
if __name__ == "__main__":
    f = None
    writer_task = None
    total_subscribed_count = 0
    loop = asyncio.get_event_loop()

    try:
        f = FeedHandler() # Initialize FeedHandler
        logger.info(f"Processing {len(TARGET_EXCHANGES)} target exchanges...")

        # Loop through configured exchanges
        for internal_name, exchange_class in TARGET_EXCHANGES.items():
            logger.info(f"--- Processing {EXCHANGE_NAME_MAP.get(internal_name, internal_name)} ({internal_name}) ---")

            # Fetch and filter symbols for the current exchange
            symbols_to_subscribe = get_symbols_to_monitor_sync(
                exchange_class,
                internal_name,
                TARGET_QUOTE_ASSETS
            )

            if symbols_to_subscribe: # Check if list is not None and not empty
                # Chunk symbols and add feeds
                limit = SYMBOL_LIMITS.get(internal_name, DEFAULT_SYMBOL_LIMIT)
                num_symbols = len(symbols_to_subscribe)
                num_connections = math.ceil(num_symbols / limit)
                logger.info(f"[{internal_name}] Will subscribe to {num_symbols} filtered symbols. Limit/Conn: {limit}. Needs {num_connections} connection(s).")

                for i in range(num_connections):
                    start_index = i * limit
                    end_index = start_index + limit
                    symbol_chunk = symbols_to_subscribe[start_index:end_index]
                    if not symbol_chunk: continue

                    logger.info(f"[{internal_name} Conn {i+1}/{num_connections}] Adding feed for {len(symbol_chunk)} tickers...")
                    try:
                        # Add feed with the specific exchange class instance
                        f.add_feed(
                            exchange_class( # Use the class from the loop
                                symbols=symbol_chunk,
                                channels=[TICKER],
                                callbacks={TICKER: TickerCallback(ticker_handler)} # Use the single handler
                            )
                        )
                        logger.info(f"[{internal_name} Conn {i+1}/{num_connections}] Feed added successfully.")
                        total_subscribed_count += len(symbol_chunk)
                    except ValueError as ve:
                        # Handle potential errors during feed addition (e.g., invalid channel for exchange)
                        logger.error(f"[{internal_name} Conn {i+1}/{num_connections}] Failed adding feed chunk: {ve}")
                        # Decide if you want to break for this exchange or continue with other chunks
                        # break
                    except Exception as e:
                        logger.error(f"[{internal_name} Conn {i+1}/{num_connections}] Unexpected error adding feed chunk: {e}", exc_info=True)
                        # break

            elif symbols_to_subscribe is None:
                 # Log if the initial symbol fetching/filtering failed for this exchange
                 logger.error(f"[{internal_name}] Skipping feed addition due to failure fetching/filtering symbols.")
            else:
                 # Log if filtering resulted in no symbols for this exchange
                 logger.info(f"[{internal_name}] No symbols matched target quotes. No feed added.")

        # Start processes only if at least one symbol was configured across all exchanges
        if total_subscribed_count > 0:
            logger.info(f"\nSuccessfully configured a total of {total_subscribed_count} symbols across {len(f.feeds)} connection(s) for {len(TARGET_EXCHANGES)} exchanges.")
            logger.info(f"Creating background task for {JSON_FILENAME} (Interval: {WRITE_INTERVAL}s).")
            writer_task = loop.create_task(periodic_json_writer(JSON_FILENAME, WRITE_INTERVAL))

            # LOG CHECK
            if writer_task:
                logger.info("Writer task object created successfully.") # LOG C
            else:
                logger.error("!!! Writer task creation FAILED (returned None/False) !!!") # LOG D

            logger.info(f"Starting feed handler...")
            logger.info("Press CTRL+C to stop.")

            # LOG CHECK
            logger.info(">>>>>>>>> ABOUT TO CALL F.RUN() <<<<<<<<<")
            f.run() # Start listening
            logger.info(">>>>>>>>>>>> F.RUN() EXECUTED <<<<<<<<<<<<")

        else:
            # Exit if no symbols could be subscribed on any exchange
            logger.error("\nNo symbols successfully configured for subscription across any exchange. Exiting.")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("\nShutdown requested via KeyboardInterrupt...")
    except SystemExit as e:
        logger.info(f"Exiting script (code {e.code}).")
    except Exception as e:
        logger.exception(f"\nCritical error in main execution: {e}")
    finally:
        if writer_task and not writer_task.done():
            logger.info("Cancelling JSON writer task...")
            writer_task.cancel()
        logger.info("Script finished.")