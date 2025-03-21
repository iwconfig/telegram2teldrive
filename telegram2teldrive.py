import os
import argparse
import tomllib
import logging
import mimetypes
import json
import uuid
import hashlib
import psycopg2
from rich.logging import RichHandler
from rich import print
from datetime import datetime
from psycopg2 import sql, IntegrityError
from telethon import TelegramClient
from telethon.tl.types import InputMessagesFilterDocument

# Configure rich logging
logging.basicConfig(level=logging.INFO, format='%(message)s', handlers=[RichHandler()])
logger = logging.getLogger('rich')

# Map string log levels to logging constants
log_levels = {
  'DEBUG': logging.DEBUG,
  'INFO': logging.INFO,
  'WARNING': logging.WARNING,
  'ERROR': logging.ERROR,
  'CRITICAL': logging.CRITICAL,
}


def load_toml_config(path):
  """Load a TOML file from the given path and return a dictionary."""
  try:
    with open(path, 'rb') as f:
      config = tomllib.load(f)
      return config
  except Exception as exc:
    logger.error(f"Failed to load TOML config file '{path}': '{exc}'")
    return {}


def setup_argparser(config):
  """
  Setup argparse using values from the provided config dictionary.
  The config is expected to have keys based on your TOML structure. For example:

  [telegram]
  api-id = "12345"

  [database]
  name = "mydb"
  """
  parser = argparse.ArgumentParser(
    description='Find existing files in Telegram channels that have not been uploaded using Teldrive, and add them to the Teldrive DB.',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
  )

  # Utility function to safely retrieve values from the TOML config dictionary.
  def get_config(section, key, fallback=None):
    return config.get(section, {}).get(key, fallback)

  parser.add_argument(
    '--log-level',
    type=str,
    help='Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).',
    default=os.getenv('LOG_LEVEL', 'INFO'),
  )
  parser.add_argument(
    '--config',
    type=str,
    help='Path to configuration file',
    default=os.getenv('CONFIG_FILE', None),
  )
  parser.add_argument(
    '--api-id',
    type=str,
    help='Telegram API ID',
    default=os.getenv('API_ID', get_config('telegram', 'api-id')),
  )
  parser.add_argument(
    '--api-hash',
    type=str,
    help='Telegram API Hash',
    default=os.getenv('API_HASH', get_config('telegram', 'api-hash')),
  )
  parser.add_argument(
    '--phone-number',
    type=str,
    help='Telegram Phone Number',
    default=os.getenv('PHONE_NUMBER', get_config('telegram', 'phone-number')),
  )
  parser.add_argument(
    '--db-name',
    type=str,
    help='Database name',
    default=os.getenv('DB_NAME', get_config('database', 'name')),
  )
  parser.add_argument(
    '--db-user',
    type=str,
    help='Database user',
    default=os.getenv('DB_USER', get_config('database', 'user')),
  )
  parser.add_argument(
    '--db-password',
    type=str,
    help='Database password',
    default=os.getenv('DB_PASSWORD', get_config('database', 'password')),
  )
  parser.add_argument(
    '--db-host',
    type=str,
    help='Database host',
    default=os.getenv('DB_HOST', get_config('database', 'host', 'localhost')),
  )
  parser.add_argument(
    '--db-port',
    type=str,
    help='Database port',
    default=os.getenv('DB_PORT', get_config('database', 'port', '5432')),
  )
  parser.add_argument(
    '--folder-name',
    type=str,
    help='Name of the base folder to create',
    default=os.getenv(
      'FOLDER_NAME', get_config('teldrive', 'folder_name', 'Imported')
    ),
  )
  parser.add_argument(
    '--channels',
    type=str,
    help='Comma-separated list of channel IDs to search',
    default=os.getenv('CHANNELS', get_config('telegram', 'channels')),
  )

  return parser.parse_args()


def setup_configuration():
  """Setup configuration from environment variables, config file, and command-line arguments."""
  # Load configuration from file provided via the CONFIG_FILE environment variable
  config = {}
  
  # Check for default config files
  default_config_files = ['telegram2teldrive.toml', 'telegram2teldrive.conf']
  for config_file in default_config_files:
      if os.path.isfile(config_file):
          logger.info(f"Loading config from default file: '{config_file}'")
          config = load_toml_config(config_file)
          break
  
  # Check for environment variable
  env_config_file = os.getenv('CONFIG_FILE')
  if env_config_file:
      logger.info(f"Loading config from ENV file: '{env_config_file}'")
      config = load_toml_config(env_config_file)
  
  args = setup_argparser(config)
  
  # If config file provided via the command-line argument --config, load it and re-parse args.
  if args.config:
      logger.info(f"Loading config from provided file: '{args.config}'")
      config = load_toml_config(args.config)
      args = setup_argparser(config)

  # Get the logging level from the command line argument or environment variable
  logger.setLevel(log_levels.get(args.log_level.upper(), logging.INFO))

  logger.info('Configuration Parameters:')
  logger.info(f'Telegram API ID: {args.api_id}')
  logger.info(f'Telegram API Hash: {args.api_hash}')
  logger.info(f'Telegram Phone Number: {args.phone_number}')
  logger.info(f'Database Name: {args.db_name}')
  logger.info(f'Database User: {args.db_user}')
  logger.info(f'Database Password: {args.db_password}')
  logger.info(f'Database Host: {args.db_host}')
  logger.info(f'Database Port: {args.db_port}')
  logger.info(f'Folder Name: {args.folder_name}')
  logger.info(f'Channels: {args.channels}')
  logger.info(f'Log Level: {args.log_level}')

  return config, args


def db_connect():
  """Establish a connection to the PostgreSQL database."""
  logger.debug('Connecting to the Teldrive database...')
  return psycopg2.connect(
    host=args.db_host,
    port=args.db_port,
    user=args.db_user,
    dbname=args.db_name,
    password=args.db_password,
  )


def fetch_one(query, params=None):
  """Fetch a single record from the database."""
  try:
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute(query, params)
    return cursor.fetchone()
  except Exception as e:
    logger.error(f'An error occurred while fetching data: {e}')
    return None
  finally:
    if cursor:
      cursor.close()
    if conn:
      conn.close()


def fetch_all(query, params=None):
  """Fetch all records from the database."""
  try:
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute(query, params)
    return cursor.fetchall()
  except Exception as e:
    logger.error(f'An error occurred while fetching data: {e}')
    return []
  finally:
    if cursor:
      cursor.close()
    if conn:
      conn.close()


def execute_query(query, params):
  """Execute a query that modifies the database (INSERT, UPDATE, DELETE)."""
  try:
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute(query, params)
    conn.commit()
    logger.debug(f'DB query executed successfully. Parameters: {params}')
  except IntegrityError as e:
    # Check if the error is due to a unique constraint violation
    if 'unique constraint' in str(e):
      logger.warning('A record with the same name already exists in this location.')
    else:
      logger.error(f'An error occurred while executing query: {e}')
  except Exception as e:
    logger.error(f'An error occurred while executing query: {e}')
  finally:
    if cursor:
      cursor.close()
    if conn:
      conn.close()


async def get_root_folder_id():
  """Get the ID of the root folder."""
  query = sql.SQL(
    'SELECT id FROM files WHERE name = %s AND type = %s and parent_id IS NULL'
  )
  result = fetch_one(query, ('root', 'folder'))
  return result[0] if result else None


async def get_channels_from_db():
  """Get channel information along with user names."""
  query = sql.SQL(
    """
        SELECT c.channel_id, c.channel_name, c.user_id, u.name, u.user_name
        FROM channels c
        JOIN users u ON c.user_id = u.user_id
    """
  )
  return fetch_all(query)


async def create_folder_in_db(parent_id, user_id, folder_name):
  """Create a new folder in the database."""
  folder_id = str(uuid.uuid4())  # Generate a new UUID
  folder_data = {
    'name': folder_name,
    'type': 'folder',
    'mime_type': 'drive/folder',
    'size': None,  # Size is NULL
    'user_id': user_id,
    'status': 'active',
    'channel_id': None,  # Not needed for folders
    'parts': None,  # Parts is NULL
    'created_at': datetime.now(),  # Use current time for created_at
    'updated_at': datetime.now(),  # Use current time for updated_at
    'encrypted': 'f',
    'category': None,  # Category is NULL
    'id': folder_id,
    'parent_id': parent_id,
  }

  insert_query = sql.SQL(
    """
        INSERT INTO files (
            name, type, mime_type, size, user_id, status, channel_id, parts,
            created_at, updated_at, encrypted, category, id, parent_id
        ) VALUES (
            %(name)s, %(type)s, %(mime_type)s, %(size)s, %(user_id)s, %(status)s,
            %(channel_id)s, %(parts)s, %(created_at)s, %(updated_at)s, %(encrypted)s,
            %(category)s, %(id)s, %(parent_id)s
        )
        """
  )

  execute_query(insert_query, folder_data)
  logger.info(f"Folder created in DB: '{folder_data['name']}' (ID: {folder_id})")


async def get_or_create_folder(user_id, folder_name, parent_id=None):
  """Get the ID of the folder for storing files, creating it if it doesn't exist."""
  if parent_id is None:
    parent_id = await get_root_folder_id()

  # Check if the folder already exists
  query = sql.SQL(
    "SELECT id FROM files WHERE name = %s AND type = 'folder' AND user_id = %s AND parent_id IS NOT DISTINCT FROM %s"
  )
  existing_folder = fetch_one(query, (folder_name, user_id, parent_id))

  if existing_folder:
    # Folder exists, return its ID
    logger.info(f"Folder '{folder_name}' already exists (ID: {existing_folder[0]}).")
    return existing_folder[0]
  else:
    # Folder does not exist, create it
    await create_folder_in_db(parent_id, user_id, folder_name)  # Pass parent_id
    # Fetch the newly created folder ID
    new_folder_id = fetch_one(query, (folder_name, user_id, parent_id))[0]
    logger.info(f"Created new folder '{folder_name}' (ID: {new_folder_id}).")
    return new_folder_id


async def get_users():
  """Get user information."""
  query = sql.SQL('SELECT user_id, name FROM users')
  return fetch_all(query)


async def add_channel_if_not_exists(channel_id, channel_name, user_id):
  """Add a channel to the database if it does not already exist."""
  insert_query = sql.SQL(
    """
        INSERT INTO channels (channel_id, channel_name, user_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (channel_id) DO NOTHING
    """
  )
  execute_query(insert_query, (channel_id, channel_name, user_id))
  logger.info(f'Channel added to DB: {channel_name} (ID: {channel_id})')


async def get_all_channels():
  """Fetch all channels the user is a part of."""
  channels = []
  async for dialog in client.iter_dialogs():
    if dialog.is_channel:
      channel = dialog.entity
      channels.append((channel.id, channel.title))
  logger.info(f'Fetched {len(channels)} channels.')
  return channels


async def fetch_channel_name_from_telegram(channel_id):
  """Fetch the channel name directly from Telegram using the channel ID."""
  try:
    channel = await client.get_entity(channel_id)
    return channel.title
  except Exception as e:
    logger.error(
      f'Failed to fetch channel name from Telegram for channel ID {channel_id}: {e}'
    )
    raise e


def parse_channel_selection(selection, max_index):
  """Parse the user input for channel selection."""
  selected_channels = set()
  parts = selection.split(',')

  for part in parts:
    part = part.strip()
    if '-' in part:  # Handle ranges
      start, end = part.split('-')
      try:
        start = int(start.strip()) - 1  # Convert to zero-based index
        end = int(end.strip()) - 1
        if start < 0 or end < 0 or start > max_index or end > max_index or start > end:
          logger.warning(f'Invalid range: {part}')
          continue
        selected_channels.update(range(start, end + 1))
      except ValueError:
        logger.error(f'Invalid range: {part}')
        return None
    else:  # Handle single numbers
      try:
        index = int(part.strip()) - 1  # Convert to zero-based index
        if 0 <= index <= max_index:
          selected_channels.add(index)
        else:
          logger.warning(f'Invalid channel number: {part}')
          return None

      except ValueError:
        logger.error(f'Invalid channel number: {part}')
        return None

  return selected_channels


async def select_channel():
  """Display channels and allow the user to select one or more channels."""
  channels = await get_all_channels()

  if not channels:
    logger.warning('No channels found.')
    print('No channels found.')
    return None

  # Display help information
  help_message = (
    '\nSelection Format:\n'
    ' - Enter a single number (e.g., 1) to select a channel.\n'
    ' - Enter multiple numbers separated by commas (e.g., 1,4,5) to select multiple channels.\n'
    ' - Enter a range using a hyphen (e.g., 1-3) to select a range of channels.\n'
    ' - Combine selections (e.g., 1,4,5-8) to select multiple channels and ranges.\n'
    ' - Enter 0, "a" or "all" to search all channels (default).\n'
    " - Type 'help', 'h', or '?' for this help message."
  )

  print("\nSelect one or more channel (enter 'h' for selection info):\n")
  print(' 0. Search all channels')
  for index, (channel_id, channel_name) in enumerate(channels):
    print(f' {index + 1}. {channel_id} ({channel_name})')

  # Get user input for channel selection
  while True:
    choice = input(
      '\nEnter the numbers of the channels you want to select [return key = 0 ("all")]: '
    )
    print() # Print an empty line

    if choice in ['help', 'h', '?']:
      print(help_message)
      continue
    if choice in ('', '0', 'a', 'all'):
      logger.info('User chose to search all channels.')
      selected_indices = set(range(0,len(channels)))
    else:
      selected_indices = parse_channel_selection(choice, len(channels) - 1)

    if selected_indices:
      logger.info(f'User selected channels: {set(i + 1 for i in selected_indices)}')
      return [channels[i][0] for i in selected_indices]  # Return list of channel_ids
    else:
      logger.warning('No valid channels selected. Please try again.')


def get_category(file_name, mime_type=None):
  """Determine the category of the file based on its MIME type or file name."""
  # If no MIME type given, try to guess it based on the file name.
  if mime_type is None:
    mime_type, _ = mimetypes.guess_type(file_name)
  if mime_type:
    mime_type = mime_type.lower()
    if mime_type.startswith('image/'):
      return 'image'
    if mime_type.startswith('video/'):
      return 'video'
    if mime_type.startswith('audio/'):
      return 'audio'
    if mime_type.startswith('text/') or mime_type == 'application/pdf':
      return 'document'
    if mime_type in {
      'application/zip',
      'application/x-tar',
      'application/x-bzip2',
      'application/x-7z-compressed',
    }:
      return 'archive'
  return 'other'


async def add_file_to_db(file_metadata, user_id, channel_id, parent_id):
  """Add a file's metadata to the database."""
  insert_query = sql.SQL(
    """
        INSERT INTO files (
            name, type, mime_type, size, user_id, status, channel_id, parts,
            created_at, updated_at, encrypted, category, id, parent_id
        ) VALUES (
            %(name)s, %(type)s, %(mime_type)s, %(size)s, %(user_id)s, %(status)s,
            %(channel_id)s, %(parts)s::jsonb, %(created_at)s, %(updated_at)s, %(encrypted)s,
            %(category)s, %(id)s, %(parent_id)s
        )
    """
  )

  # Prepare the file data for insertion
  file_data = {
    'name': file_metadata['file_name'],
    'type': 'file',  # Assuming all downloaded items are files
    'mime_type': file_metadata['mime_type'],
    'size': file_metadata['file_size'],
    'user_id': user_id,
    'status': 'active',
    'channel_id': channel_id,
    'parts': json.dumps([{'id': file_metadata['message_id']}]),
    'created_at': datetime.now(),
    'updated_at': datetime.now(),
    'encrypted': False,  # Assuming files are not encrypted
    'category': get_category(file_metadata['file_name'], file_metadata['mime_type']),
    'id': str(uuid.uuid4()),  # Generate a new UUID for the file
    'parent_id': parent_id,  # Use the parent folder ID
  }

  execute_query(insert_query, file_data)
  logger.info(
    f"File metadata added to Teldrive DB: '{file_data['name']}' (Telegram Message ID: {file_metadata['message_id']})"
  )


def check_file_exists(message_id, user_id, channel_id, parent_id, file_name):
  """Check if a file with the given metadata already exists in the database."""
  query = sql.SQL(
    'SELECT id, name FROM files WHERE parts @> %s::jsonb AND user_id = %s AND channel_id = %s AND parent_id = %s'
  )
  result = fetch_one(
    query, (json.dumps([{'id': message_id}]), user_id, channel_id, parent_id)
  )

  if result:
    if result[1] != file_name:
      logger.warning(
        f"File with message ID '{message_id}' already exists with a different name: {result[1]}"
      )
    else:
      logger.info(
        f"File with message ID '{message_id}' already exists in the Teldrive DB."
      )

  return result


async def main():
  """Main function to run the Telegram client and process messages."""

  me = await client.get_me()
  logger.info(f'User Name: {me.first_name}, User ID: {me.id}')

  # Select a channel
  if args.channels:
    if args.channels.lower() == 'all':
      channel_ids_to_search = [channel[0] for channel in await get_all_channels()]
    else:
      channel_ids_to_search = [int(id.strip()) for id in args.channels.split(',')]
  else:
    channel_ids_to_search = await select_channel()

  if channel_ids_to_search is None:
    logger.info('Proceeding with all channels.')
  logger.info(f'Selected channel IDs: {channel_ids_to_search}')

  # Fetch channels for the selected user
  channels = await get_channels_from_db()
  channel_ids = {channel[0] for channel in channels}  # Set of existing channel IDs
  logger.debug(f'Teldrive DB (channels): {channels}')
  logger.info(
    f'Existing channels in Teldrive DB: {", ".join(f"{channel[0]} ({channel[1]})" for channel in channels)}'
  )

  added_channels = set()  # To keep track of added channels

  # Ensure the "imported" root folder exists and get its ID
  base_folder_id = await get_or_create_folder(me.id, args.folder_name)

  for channel_id in channel_ids_to_search:
    # Ensure the subfolder for the channel exists and get its ID
    channel_name = await fetch_channel_name_from_telegram(channel_id)
    channel_folder_id = await get_or_create_folder(
      me.id, f'channel_{channel_id}_{channel_name}', base_folder_id
    )

    logger.info(f'Processing messages from channel ID: {channel_id}')
    async for message in client.iter_messages(
      channel_id, filter=InputMessagesFilterDocument
    ):
      # Check if the channel exists in the database
      if channel_id not in channel_ids and channel_id not in added_channels:
        channel_name = message.chat.title if message.chat.title else 'Unknown Channel'
        # Add the channel to the database
        await add_channel_if_not_exists(channel_id, channel_name, me.id)
        added_channels.add(channel_id)  # Mark this channel as added
        logger.info(f"Added new channel: '{channel_name}' (ID: {channel_id})")

      # Collect metadata
      file_name = (
        message.file.name
        if message.file.name
        else hashlib.md5(str(uuid.uuid4()).encode()).hexdigest()
      )  # Fallback name if no name is provided

      # Determine the file extension based on the MIME type
      file_extension = mimetypes.guess_extension(message.file.mime_type)
      if file_extension and not file_name.endswith(file_extension):
        file_name += file_extension  # Append the extension if it's not already present

      file_metadata = {
        'file_name': file_name,
        'file_size': message.file.size,
        'mime_type': message.file.mime_type,
        'date': message.date,
        'message_id': message.id,
        'channel_id': channel_id,  # Add channel_id to file metadata
      }

      logger.info(f"Media found: '{file_name}'")
      logger.debug(f'File metadata collected: {file_metadata}')

      # Check if the file already exists in the database
      if not check_file_exists(
        message.id, me.id, channel_id, channel_folder_id, file_name
      ):
        # Add file metadata to the database with the channel's folder ID
        await add_file_to_db(file_metadata, me.id, channel_id, channel_folder_id)


# Run the client
if __name__ == '__main__':
  _, args = setup_configuration()

  api_id = args.api_id
  api_hash = args.api_hash
  phone_number = args.phone_number

  # Create the Telegram client
  client = TelegramClient('telegram2teldrive', api_id, api_hash).start(
    phone=phone_number
  )

  with client:
    logger.info('Starting the Telegram client...')
    client.loop.run_until_complete(main())
    logger.info('Telegram client has finished processing.')
