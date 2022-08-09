from dotenv import load_dotenv
import os 
import paho.mqtt.client as mqtt #import mqtt client
import time
import json
import requests #import http requester

#### load keys
load_dotenv("../.env")

#### broker setup
broker_username1 = os.getenv('secretUsername1')
broker_pass1 = os.getenv('secretPassword1')
broker_address1=os.environ.get('secretURL1')
broker_port1=int(os.environ.get('secretPort1'))
#broker_address="iot.eclipse.org"

#### http setup
api_key1 = os.getenv('secretxapikey')
api_key_test1 = 'test_key'
http_url1 = os.getenv('http_url1')
http_url_test1 = os.getenv('http_url_test1')
http_url_test2 = 'http://httpbin.org/post'
http_url_test3 = os.getenv('http_url_test3')

headers1 = {'x-subscription-key': '{key}'.format(key=api_key1), 'Content-type':'application/json'}
headers_test = {'x-subscription-key': '{key}'.format(key=api_key_test1), 'Content-type':'application/json'}

#### uplink interval setup
interval_second = 1000
interval_minute = 60000
uplink_interval = interval_minute #10*interval_second
time_keeper = 0
time_last_publish = 0
time_ticker_new = 0

#### MQTT callbacks
def on_connect(client, userdata, flags, rc):
    print("Connected with result code", rc)
    print("Subscribing to topic","odaq/266713/01/#")
    client.subscribe("odaq/266713/01/#")

def on_message(client, userdata, message):
    #print("message topic=",message.topic)
    #print("message qos=",message.qos)
    #print("message retain flag=",message.retain)
    #print("message received=" ,str(message.payload.decode("utf-8")))

    #### Global variables
    global time_keeper, uplink_interval, time_last_publish
    global time_ticker_new

    #### subscription payload
    payload_mqtt = json.loads(str(message.payload.decode("utf-8")))

    #### get current timestamp    
    time_new = payload_mqtt['data']['from_time']
    
    #### ticker
    if time_ticker_new != time_new:
        print(time_new)
        time_ticker_new = time_new

    #### if enough time has passed to exceed uplink interval, then package data and execute HTTP POST request
    #### In order to update all the sensor payloads from 1 payload, the 'time_last_publish' is used to discern if it is all from the same payload
    if time_new - time_keeper >= uplink_interval or time_new == time_last_publish:
        time_last_publish = time_new

        # create new payload dictionary
        payload_new = {}
        sensor_name = message.topic
        payload_new['eui'] = 'arn-docomo-odaq-266713' + sensor_name[-6:]
        payload_new['format'] = "json"

        # create data object
        data = {}
        data['avg_deltalambda'] = payload_mqtt['data']['intrusion_data']['avg_deltalambda']
        data['std_dev_deltalambda'] = payload_mqtt['data']['intrusion_data']['std_dev_deltalambda']
        data['max_deltalambda'] = payload_mqtt['data']['intrusion_data']['max_deltalambda']
        data['min_deltalambda'] = payload_mqtt['data']['intrusion_data']['min_deltalambda']
        data['intrusion_probability'] = payload_mqtt['data']['intrusion_probability']
        data['alarm'] = payload_mqtt['data']['alarm']
        data['alarm_enum'] = payload_mqtt['data']['alarm_enum']

        # load data object into payload_new dictionary
        payload_new['data'] = data

        # update time_keeper to current timestamp
        time_keeper = time_new

        # send HTTP POST request with payload_new
        
        #r = requests.post(http_url_test3, headers=headers_test, data=json.dumps(payload_new))
        r = requests.post(http_url1, headers=headers1, json=json.dumps(payload_new))
        #sonData = r.json()
        #print(jsonData)
        print("status code: ", r.status_code)
        #print("\n")

########################################

print("creating new instance")
client1 = mqtt.Client("P1", clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp") #create new instance
client1.on_connect=on_connect
client1.on_message=on_message #attach function to callback

#### needed is broker requires TLS
#client.tls_set()  # <--- even without arguments

#set access credentials
print("entering credentials")
client1.username_pw_set(broker_username1, broker_pass1)
print("connecting to broker")
client1.connect(broker_address1, broker_port1) #connect to broker
print("Connecting...")

#### moved to on_connect callback
#client.subscribe("odaq/266713/01/#")

#### publishing function
#print("Publishing message to topic","house/bulbs/bulb1")
#client.publish("house/bulbs/bulb1","OFF")

#### if looping forever
client1.loop_forever()

#### for looping preset time
#client1.loop_start() #start the loop
#time.sleep(30) # wait
#client1.loop_stop() #stop the loop


