import time, os, shutil
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta

file_list = []
for file in os.listdir('/app/ltip_drdl_hourly_volume_data'):
        if("ltip_drdl" in file):        file_list.append(file);

for file in file_list:  shutil.move("/app/ltip_drdl_hourly_volume_data/%s" % file, "/app/ltip_drdl_hourly_volume_data/old/%s" % file)

prev_1_hour = (datetime.now() - timedelta(hours=5)).strftime("%Y-%m-%dT%H")
time_gte = prev_1_hour + ":00:00.000Z"
time_lte = prev_1_hour + ":59:00.000Z"
print(time_gte)
print(time_lte)

ltip_drdl = {"Facebook Video":"Facebook video", "Facebook":"Facebook", "Instagram":"Instagram", "Snapchat":"Snapchat", "WhatsApp Media":"WhatsApp TLS",
"Dailymotion":"Daily Motion", "OSN Play":"OSN Play", "WhatsApp":"WhatsApp", "Facebook Messenger":"Facebook Messenger", "Facebook Voice":"Facebook VoIP",
"WhatsApp Call":"WhatsApp voice", "Whatsapp Video Call":"WhatsApp Video Call", "Facebook Live":"Facebook Live", "Instagram Live":"Instagram Live",
"OSN":"OSN", "Instagram Video":"Instagram Video", "YouTube":"YouTube", "Netflix":"Netflix", "TikTok":"Tik Tok", "iTunes App Store":"iTunes Store",
"Windows App Store":"Windows Store", "Google":"Google", "Microsoft Teams Call":"Microsoft Teams Call", "Apple Updates":"Apple Software Update",
"ZOOM":"Zoom", "iCloud":"iCloud", "SHAHID":"Shahid", "YouTube Web":"YouTube Web", "Amazon Web Services":"Amazon AWS", "Twitch":"Twitch",
"Google Cloud Storage":"Google Cloud Storage", "Telegram":"Telegram", "Google Generic Call":"Google Generic Call", "Microsoft Outlook 365":"Outlook 365",
"Outlook.com":"Outlook.com", "WebRTC":"WebRTC", "Twitter Video":"Twitter video", "Skype":"Skype", "ROBLOX":"ROBLOX",
"Xbox One Games Download":"Xbox Live update", "Gmail":"Gmail", "Microsoft Teams":"Microsoft Teams", "OpenVPN":"OpenVPN",
"PC: League Of Legends":"League of Legends", "Amazon":"Amazon", "ToTok Call":"TotokCall", "Twitter":"Twitter", "Dropbox":"Dropbox", "Reddit":"Reddit",
"PC: Fortnite":"Fortnite", "BOT Call":"BotIM Voice", "HiU Call":"HiuCall", "Voico Call":"VoicoCall"
}

timestr = time.strftime("%Y-%m-%d %H:%M")
file_timestr = time.strftime("%Y-%m-%d_%H%M")
report_csv = '/app/ltip_drdl_hourly_volume_data/ltip_drdl_hourly_volume_data-%s.csv' % file_timestr
file_csv = open(report_csv, 'w+')
file_csv.write('Date,Attribute,Value\n')

dict_count = len(ltip_drdl)
i = 1;

es_ltip = Elasticsearch([{'host': '10.237.226.243', 'port': 9200}])
#print(es_ltip)
es_ltip.indices.put_settings(index="top30apps-*", body= {"index" : {"max_result_window" : 50000}})
res_ltip = es_ltip.search(index='top30apps-*',size=50000,sort="Measurement Date:desc",body={"query": {"bool":{"must":[{"range": {"@timestamp": {"gte": "%s" % time_gte,"lte": "%s" % time_lte}}}]}}},request_timeout=60)

es_drdl = Elasticsearch([{'host': 'localhost', 'port': 9200}])
#print(es_drdl)
res_drdl = es_drdl.search(index='proceraallbw-*',size=10000,sort="Measurement Date:desc",body={"query": {"bool":{"must":[{"range": {"@timestamp": {"gte": "%s" % time_gte,"lte": "%s" % time_lte}}}]}}},request_timeout=60)

for key, value in ltip_drdl.items():
        print("#########################################")
        #es_ltip = Elasticsearch([{'host': '10.237.226.243', 'port': 9200}])
        #print(es_ltip)
        #es_ltip.indices.put_settings(index="top30apps-2021", body= {"index" : {"max_result_window" : 50000}})

        #res_ltip = es_ltip.search(index='top30apps-*',size=50000,sort="Measurement Date:desc",body={"query": {"bool":{"must":[{"range": {"@timestamp": {"gte": "%s" % time_gte,"lte": "%s" % time_lte}}}]}}},request_timeout=60)
        #print(res)
        data = res_ltip['hits']['hits']
        #print(data)
        print("--------(" + str(i) + "/" + str(dict_count) + ") LTIP " + key + ", DRDL " + value +  " --------")
        total_volume_ltip = 0
        for num, doc in enumerate(data):
                message = doc['_source']['message']
                if (str(message).split(',')[0].endswith(key)):
                        #print (num, '--', message)
                        total_volume_ltip += int(str(message).split(',')[3])
        file_csv.write(timestr + "," + "LTIP-" + key + "," + str(total_volume_ltip) + "\n")
        #es_drdl = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        #print(es_drdl)
        #res_drdl = es_drdl.search(index='proceraallbw-*',size=10000,sort="Measurement Date:desc",body={"query": {"bool":{"must":[{"range": {"@timestamp": {"gte": "%s" % time_gte,"lte": "%s" % time_lte}}}]}}},request_timeout=60)
        #print(res_drdl)
        data = res_drdl['hits']['hits']
        #print(data)

        total_volume_drdl = 0
        for num, doc in enumerate(data):
                message = doc['_source']['message']
                if(str(message).split(',')[4].endswith(value)):
                        #print (num, '--', message)
                        total_volume_drdl = total_volume_drdl + int(str(message).split(',')[5]) + int(str(message).split(',')[6])

        print(str(total_volume_ltip) + "," + str(total_volume_drdl))
        i += 1
        file_csv.write(timestr + "," + "DRDL-" + value + "," + str(total_volume_drdl) + "\n")
print("#########################################")

file_csv.close()