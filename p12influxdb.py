#!/usr/bin/python3

import requests
import json
import os
from json import JSONEncoder
from influxdb import InfluxDBClient

STATE_FILE = '/var/lib/p12influxdb/state'
P12INFLUXDB = 'p12influxdb'
P1HOST = '192.168.178.46'
P1PORT = 80
client = InfluxDBClient('localhost', 8086, 'root', 'root', 'P12INFLUXDB')
client.switch_database( P12INFLUXDB)

offset = 0
maxcount = 25
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, 'r') as sfp:
            next_uri = sfp.readline()
else:
    next_uri = 'http://{}:{}/api/v2/datalogger/dsmrreading?limit=25&offset={}'.format(P1HOST,P1PORT,offset)

while next_uri :
    uri = next_uri
    #print( 'getting next uri: %s'% next_uri)
    response = requests.get(uri,headers={'X-AUTHKEY': '7S2PSY50Y7SL7WNJFFK5CGX8A5KPQV1NU42D3E3C7IT1HJD0E0S2X2W6ZHLV2POR'},)
    if response.status_code != 200:
    #    print ('status_code was %s'% response.status_code)
        uri = next_uri
        with open(STATE_FILE, 'w') as wfp:
            wfp.write(uri)
        next_uri = None
        break
    else:
        #print (response.text)
        data = json.loads(response.text)
        if data['next']:
            # Due to a proxy issue, I have an unwanted substring
            next_uri = data['next'].replace('dsmrreader/', '')
        else:
            next_uri = None

        dsmr=[]
        for i in range(len(data["results"])):
            dsmr.append( {
              "measurement" : "dsmrreading",
              "time": data["results"][i]["timestamp"],
              "tags": {
                 "source": "p12influxdb"
              },
              "fields": {
                 "electricity_currently_delivered" : float(data["results"][i]["electricity_currently_delivered"]),
                 "electricity_delivered_1" : float(data["results"][i]["electricity_delivered_1"]),
                 "electricity_delivered_2" : float(data["results"][i]["electricity_delivered_2"]),
                 "id" : data["results"][i]["id"]
              }
            }
            )
        client.write_points(dsmr)
        #print(json.dumps(dsmr, cls=ReadingEncoder))
    offset = offset+25

#response = requests.get(
#    'http://holder.timt.org:8080/api/v2/datalogger/dsmrreading?limit=200000',
#    headers={'X-AUTHKEY': '7S2PSY50Y7SL7WNJFFK5CGX8A5KPQV1NU42D3E3C7IT1HJD0E0S2X2W6ZHLV2POR'},
#)
