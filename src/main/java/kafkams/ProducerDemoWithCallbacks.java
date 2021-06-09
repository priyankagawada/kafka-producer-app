package kafkams;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallbacks {

	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);
		System.out.println("Producer Application");

		// create producer properties
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("key.serializer", StringSerializer.class.getName()); // what type of value u are sending
																					// and how to convert into bytes
		properties.setProperty("value.serializer", StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		// create a producer record
		// First String - topic name
		// Second String - String message to be produced
		for(int i = 0; i < 10 ; i++) {
			
			String topic = "api_topic";
			String key = "alice";
			String value = "Hello - " + Integer.toString(i); 
	
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
				

		// send data - asynchronous method that runs in the background
		producer.send(record, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// TODO Auto-generated method stub

				if (exception == null) {
					// record was successfully sent
					logger.info("Received new Metadata: \n" + "Topic - " + metadata.topic() + "\n" + "partition - "
							+ metadata.partition() + "\n" + "offset - " + metadata.offset() + "\n" + "timestamp - "
							+ metadata.timestamp());

				} else {
                   logger.error("Error while producing - " + exception);
				}
			}

		});

		}
		
		// flush data and close producer
		producer.flush();
		producer.close();

	}

}
