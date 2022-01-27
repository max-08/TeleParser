# import libraries
import configparser
import json
import pandas as pd
import os
import pymorphy2
import nltk
from nltk.stem import WordNetLemmatizer 
import csv
import sys
from math import *
from pymongo import MongoClient

maxInt = sys.maxsize

while True:
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/2)

from telethon.sync import TelegramClient
from telethon import connection

# to correctly transfer message times to json
from datetime import date, datetime

# classes for working with channels
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch

# class for working with messages
from telethon.tl.functions.messages import GetHistoryRequest

# Read credentials
config = configparser.ConfigParser()
config.read("config.ini")

# Assign values to internal variables
api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']
username = config['Telegram']['username']

# create a Telegram client object
client = TelegramClient(username, api_id, api_hash)

client.start()

async def dump_all_participants(channel):
	"""Writes a json file with information about all channel/chat members"""
	offset_user = 0    # member number from which reading starts
	limit_user = 100   # maximum number of records transferred at one time

	all_participants = []   # list of all channel members
	filter_user = ChannelParticipantsSearch('')

	while True:
		participants = await client(GetParticipantsRequest(channel,
			filter_user, offset_user, limit_user, hash=0))
		if not participants.users:
			break
		all_participants.extend(participants.users)
		offset_user += len(participants.users)

async def dump_all_messages(channel, short_url_, datetime_):
	"""Writes a json file with information about all channel/chat messages"""
	offset_msg = 0    # record number from which to start reading
	limit_msg = 100   # maximum number of records transferred at one time

	all_messages = []   # list of all messages
	total_messages = 0
	total_count_limit = 0  # change this value if you don't need all messages
    
	class DateTimeEncoder(json.JSONEncoder):
		'''Class for serializing date records to JSON'''
		def default(self, o):
			if isinstance(o, datetime):
				return o.isoformat()
			if isinstance(o, bytes):
				return list(o)
			return json.JSONEncoder.default(self, o)

	while True:
		history = await client(GetHistoryRequest(
			peer=channel,
			offset_id=offset_msg,
			offset_date=None, add_offset=0,
			limit=limit_msg, max_id=0, min_id=0,
			hash=0))
		if not history.messages:
			break
		messages = history.messages
		for message in messages:
			all_messages.append(message.to_dict())
		offset_msg = messages[len(messages) - 1].id
		total_messages = len(all_messages)
		print(f'{str(datetime.now())} | Получено записей: {len(all_messages)}', end='\r')
		if total_count_limit != 0 and total_messages >= total_count_limit:
			break

	with open(f'{short_url_}_messages_{datetime_}.json', 'w', encoding='utf8') as outfile:
		json.dump(all_messages, outfile, ensure_ascii=False, cls=DateTimeEncoder)

async def main():
	channel = await client.get_entity(url)
	#await dump_all_participants(channel)
	await dump_all_messages(channel, channel_string, datetime_string)

# downloading required components
nltk.download('wordnet')
nltk.download('punkt')

# clearing the console of unnecessary information
cls = lambda: os.system('cls')
cls()

# parsing a chat or channel in Telegram and saving to a JSON file
url = 't.me/' + input("Enter a link to a channel or chat: @")
#url = 't.me/testflight_app'
channel_string = url.split('/')[-1]
print(f'{str(datetime.now())} | Parsing started')
datetime_string = str(datetime.now()).replace("-", "").replace(" ", "T").replace(":", "").split(".")[0]
with client:
	client.loop.run_until_complete(main())
print(f'{str(datetime.now())} | Parsing finished!')

# copying JSON file content to CSV file
print(f'{str(datetime.now())} | Started copying JSON file content to CSV file')
json_file = pd.read_json(f'{channel_string}_messages_{datetime_string}.json')
json_file.to_csv(f'{channel_string}_messages_{datetime_string}.csv', index = None, encoding = 'utf8')
print(f'{str(datetime.now())} | Copying the contents of the JSON file to the CSV file is complete!')

# importing json file content into database
#mongoimport_path = '"C:\\Program Files\\MongoDB\\Server\\4.4\\bin\\mongodb-database-tools-windows-x86_64-100.3.1\\bin\\mongoimport.exe"'
#os.popen(f'{mongoimport_path} -d KM5_BigData -c {channel_string}_{datetime_string} --file {channel_string}_messages_{datetime_string}.json --jsonArray')
print(f'{str(datetime.now())} | Started importing JSON file content into database')
db_original = MongoClient('mongodb://127.0.0.1:27017')['KM5_BigData'][f'{channel_string}_{datetime_string}']
with open(f'{channel_string}_messages_{datetime_string}.json', 'r', encoding='utf8') as json_file:
    json_file_data = json.load(json_file)
db_original.insert_many(json_file_data)
print(f'{str(datetime.now())} | The import of JSON file content into the database is complete!')

# deleting extra columns in the database
print(f'{str(datetime.now())} | Started deleting extra columns in the database')
db_original.update_many({}, {'$unset': {'_': '', 
										'out': '', 
										'mentioned': '', 
										'media_unread': '', 
										'silent': '', 
										'post': '', 
										'from_scheduled': '', 
										'legacy': '', 
										'edit_hide': '', 
										'pinned': '', 
										'fwd_from': '', 
										'via_bot_id': '', 
										'reply_to': '', 
										'reply_markup': '', 
										'replies': '', 
										'edit_date': '', 
										'post_author': '', 
										'grouped_id': '', 
										'restriction_reason': '', 
										'ttl_period': '', 
										'action': ''}})
print(f'{str(datetime.now())} | Removing extra columns in the database is complete!')

# леммирование текста
print(f'{str(datetime.now())} | Text lemming')
morph = pymorphy2.MorphAnalyzer()
data = []
with open(f'{channel_string}_messages_{datetime_string}.csv', 'r', encoding='utf-8', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        data.append(row)
text_to_lemm = []
for i in range(len(data)):
	text_to_lemm.append(data[i]['message'])
lemm_text_list = []
for i in range(len(text_to_lemm)):
    word_list = nltk.word_tokenize(text_to_lemm[i])
    lemm_text = ' '.join([morph.parse(w)[0].normal_form for w in word_list])
    lemm_text_list.append(lemm_text)
for i in range(len(lemm_text_list)):
	data[i]['message'] = lemm_text_list[i]
print(f'{str(datetime.now())} | Text lemming completed!')

# write lemmed text to CSV file
print(f'{str(datetime.now())} | Writing Lemmed Text to a CSV File')
fieldnames = [t for i, t in enumerate(data[0])]
with open(f'{channel_string}_messages_{datetime_string}_lemm.csv', 'w', encoding='utf-8-sig', newline='') as csvlemmfile:
	writer = csv.DictWriter(csvlemmfile, delimiter=';', fieldnames=fieldnames)
	writer.writeheader()
	for row in data:
		writer.writerow(row)
print(f'{str(datetime.now())} | Lemmed text CSV file created!')

# copying a collection for A/B testing
def CopyFromColl1ToColl2(database1, collection1, database2, collection2):
    db1 = MongoClient('mongodb://127.0.0.1:27017')[database1][collection1]
    db2 = MongoClient('mongodb://127.0.0.1:27017')[database2][collection2]
    #here you can put the filters you like.
    for a in db1.find():
        try:
            db2.insert_one(a)
            print(f'{str(datetime.now())} | Entry copied successfully', end='\r')
        except:
            print(f'{str(datetime.now())} | Copy failed')

print(f'{str(datetime.now())} | Copying a Collection for A/B Testing')
CopyFromColl1ToColl2('KM5_BigData', f'{channel_string}_{datetime_string}', 'KM5_BigData', f'{channel_string}_{datetime_string}_AB')
print(f'{str(datetime.now())} | Copying the collection for A/B testing is complete!')

# preparing the collection for A/B testing
print(f'{str(datetime.now())} | Preparing a Collection for A/B Testing')
db_for_AB = MongoClient('mongodb://127.0.0.1:27017')['KM5_BigData'][f'{channel_string}_{datetime_string}_AB']
db_for_AB_len = db_for_AB.estimated_document_count()
db_for_AB_len_A = ceil(db_for_AB_len * 0.75)
db_for_AB_len_B = db_for_AB_len - db_for_AB_len_A
db_for_AB.update_many({}, {'$set': {'flag': 'A'}})
db_for_AB.update_many({'$expr': {'$eq': [1, {'$mod': ['$id', 4]}]}}, {'$set': {'flag': 'B'}})
db_for_AB_docs_A = db_for_AB.count_documents({'flag': 'A'})
db_for_AB_docs_B = db_for_AB.count_documents({'flag': 'B'})
#print(ceil(db_for_AB_len * 0.75), '!=', db_for_AB_docs_A)
#print(db_for_AB_len - db_for_AB_len_A, '!=', db_for_AB_docs_B)
while (db_for_AB_docs_B < db_for_AB_len_B):
	db_for_AB.update_one({'flag': 'A'}, {'$set': {'flag': 'B'}})
	db_for_AB_docs_A = db_for_AB.count_documents({'flag': 'A'})
	db_for_AB_docs_B = db_for_AB.count_documents({'flag': 'B'})
while (db_for_AB_docs_A < db_for_AB_len_A):
	db_for_AB.update_one({'flag': 'B'}, {'$set': {'flag': 'A'}})
	db_for_AB_docs_A = db_for_AB.count_documents({'flag': 'A'})
	db_for_AB_docs_B = db_for_AB.count_documents({'flag': 'B'})
#print(ceil(db_for_AB_len * 0.75), '!=', db_for_AB_docs_A)
#print(db_for_AB_len - db_for_AB_len_A, '!=', db_for_AB_docs_B)
print(f'{str(datetime.now())} | Collection preparation for A/B testing completed!')

cursor = db_for_AB.find({})
df = pd.DataFrame(list(cursor))
df.to_csv(f'{channel_string}_messages_{datetime_string}_AB.csv', index=False, encoding='utf-8-sig')

csvfile = open(f'{channel_string}_messages_{datetime_string}_AB.csv', 'r', encoding='utf-8-sig')
jsonfile = open(f'{channel_string}_messages_{datetime_string}_AB.json', 'w', encoding='utf8')

reader = csv.DictReader(csvfile, delimiter=';')
jsonfile.write('[')
for row in reader:
    json.dump(row, jsonfile, ensure_ascii=False)
    jsonfile.write(',')
jsonfile.write(']')

input('Press Enter to close')
