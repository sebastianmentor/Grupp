from kafka import KafkaConsumer
import json
import time

consumer = KafkaConsumer(
    'Orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-processed-consumer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_commit_interval_ms=3000
)

try:
    while True:
        time.sleep(3)
        messages = consumer.poll()
        while (msg := messages.next()):
            print(msg[0])
            
        # for key,value in messages.items():
        #     print(len(value))
        
except KeyboardInterrupt:
    print('Stänger')
finally:
    consumer.close()

