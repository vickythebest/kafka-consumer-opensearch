package com.learn.kafka.elasticsearch;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {

	public static RestHighLevelClient createOpenSearchClient() {

		String connString = "http://ecc9202b22d75d4c9b970d610a8894ec9714bf221c51a11dfb9dfaa70b65fae4@192.168.144.101:9200";

		// we build a URI from the connection string
		RestHighLevelClient restHighLevelClient;

		URI connUri = URI.create(connString);

		// extract login information if it exists
		String userInfo = connUri.getUserInfo();

		if (userInfo == null) {
			// REST client without security
			restHighLevelClient = new RestHighLevelClient(
					RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

		} else {
			System.out.println("create connection");
			String username = "elastic";
			String password = "changeme";
			// Create a new instance of the REST High-Level client
			restHighLevelClient = new RestHighLevelClient(
					RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort()))
							.setHttpClientConfigCallback(httpClientBuilder -> {
								// Set the credentials provider
								CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
								credentialsProvider.setCredentials(AuthScope.ANY,
										new UsernamePasswordCredentials(username, password));
								httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
								return httpClientBuilder;
							}));

			System.out.println("create connection");

		}

		return restHighLevelClient;
	}

	private static KafkaConsumer<String, String> createKafkaConsumer() {

		String groupId = "consuemr-elasticsearch-demo";
//		String topic="first_topic";
		Properties properties = new Properties();

		properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule");
		properties.setProperty("sasl.machanism", "PLAIN");

//		Set producer properties
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.55.11:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		return new KafkaConsumer<>(properties);

	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		System.out.println("Main");

		Logger log = LoggerFactory.getLogger(ElasticSearchConsumer.class);

		RestHighLevelClient elasticSearchClient = createOpenSearchClient();
		KafkaConsumer<String, String> consumer = createKafkaConsumer();
//		List<String> topics = Collections.singletonList("wikimedia");

		boolean isIndexExist = false;
		try (elasticSearchClient; consumer) {

			System.out.println("isIndexExist");
			isIndexExist = elasticSearchClient.indices().exists(new GetIndexRequest("wikimedia"),
					RequestOptions.DEFAULT);
			System.out.println("isIndexExist: " + isIndexExist);

			if (!isIndexExist) {
				log.info("wikimedia not index exist");
				CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
				elasticSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
			} else {
				log.info("wikimedia index exist");
			}

			log.info("Begin while to instert data");
			consumer.subscribe(Collections.singletonList("wikimedia_recentchange"));
			while (true) {
				log.info("consumers.poll");
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

				int recordCount = records.count();

				log.info("Receive: " + recordCount + " record(s");

				for (ConsumerRecord<String, String> record : records) {
					
					
//					String id = record.topic()+"_"+record.partition()+"_"+record.offset();
					
					String id=extractId(record.value());
					
					try {
						IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);

						IndexResponse response = elasticSearchClient.index(indexRequest, RequestOptions.DEFAULT);

						log.info("_doc/Id "+response.getId());
					} catch (Exception e) {

					}
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	private static String extractId(String json) {
		
		
		return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
		
	}
}
