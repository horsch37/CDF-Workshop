# Contents
- [Lab 1](#lab-1) - Getting started with Apache NiFi
  - Consuming the Meetup RSVP stream
  - Extracting JSON elements we are interested in
  - Splitting JSON into smaller fragments
  - Writing JSON to File System
- [Lab 2](#lab-2)
  - Access the cluster  
- [Lab 3](#lab-3) - MiNiFi Java Agents with EFM
  - Designing the MiNiFi Flow
  - Preparing the flow
  - Running MiNiFi agents
- [Lab 4](#lab-4) - Kafka Basics
  - Creating a topic
  - Producing data
  - Consuming data
- [Lab 5](#lab-5) - Integrating Kafka with NiFi
  - Creating the Kafka topic
  - Adding the Kafka producer processor
  - Verifying the data is flowing
- [Lab 6](#lab-6) - Integrating the Schema Registry
  - Creating the Kafka topic
  - Adding the Meetup Avro Schema
  - Sending Avro data to Kafka

#### Login to Ambari

- Login to Ambari web UI by opening http://{YOUR_IP}:8080 and log in with **admin/StrongPassword**

- You will see a list of Hadoop components running on your node on the left side of the page
  - They should all show green (ie started) status. If not, start them by Ambari via 'Service Actions' menu for that service

#### NiFi Installation Directory

- NiFi is installed at: /usr/hdf/current/nifi

-----------------------------

# Lab 1

In this lab, we will learn how to:
  - Consume the Meetup RSVP stream
  - Extract the JSON elements we are interested in
  - Split the JSON into smaller fragments
  - Write the JSON to the file system

### Consuming RSVP Data

To get started we need to consume the data from the Meetup RSVP stream, extract what we need, splt the content and save it to a file:

#### Goals:
   - Consume Meetup RSVP stream
   - Extract the JSON elements we are interested in
   - Split the JSON into smaller fragments
   - Write the JSON files to disk

 Our final flow for this lab will look like the following:

  ![Image](https://github.com/horsch37/CDF-Workshop/raw/master/lab1.png)
  A template for this flow can be found [here](https://raw.githubusercontent.com/horsch37/CDF-Workshop/master/templates/HDF-Workshop_Lab1-Flow.xml)

  - Step 1: Add a ConnectWebSocket processor to the cavas
      - In case you are using a downloaded template, the ControllerService will be prepopulated. You will need to enable the ControllerService. Double-click the processor and follow the arrow next to the JettyWebSocketClient
      
      ws://stream.meetup.com/2/rsvps
      
      - Set WebSocket Client ID to your favorite number.
  - Step 2: Add an UpdateAttribute procesor
    - Configure it to have a custom property called ``` mime.type ``` with the value of ``` application/json ```
  - Step 3. Add an EvaluateJsonPath processor and configure it as shown below:
  ![Image](https://github.com/horsch37/CDF-Workshop/raw/master/jsonpath.png)

    The properties to add are:
    ```
    event.name    $.event.event_name

    event.url     $.event.event_url

    group.city    $.group.group_city

    group.state   $.group.group_state

    group.country $.group.group_country

    group.name    $.group.group_name

    venue.lat     $.venue.lat

    venue.lon     $.venue.lon

    venue.name    $.venue.venue_name
    ```
  - Step 4: Add a SplitJson processor and configure the JsonPath Expression to be ```$.group.group_topics ```
  - Step 5: Add a ReplaceText processor and configure the Search Value to be ```([{])([\S\s]+)([}])``` and the Replacement Value to be
    ```
    {
      "event_name": "${event.name}",
      "event_url": "${event.url}",
      "venue" : {
      	"lat": "${venue.lat}",
      	"lon": "${venue.lon}",
      	"name": "${venue.name}"
      },
      "group": {
        "group_city": "${group.city}",
        "group_country": "${group.country}",
        "group_name": "${group.name}",
        "group_state": "${group.state}",
        $2
      }
    }
    ```
  - Step 6: Add a PutFile processor to the canvas and configure it to write the data out to ```/tmp/rsvp-data```

##### Questions to Answer
1. What does a full RSVP Json object look like?
2. How many output files do you end up with?
3. How can you change the file name that JSON is saved as from PutFile?
3. Why do you think we are splitting out the RSVP's by group?
4. Why are we using the UpdateAttribute processor to add a mime.type?
4. How can you change the flow to get the member photo from the JSON file and download it.


------------------

# Lab 2

## Accessing your Cluster

Credentials will be provided for these services by the instructor:

* SSH
* Ambari

## Use your Cluster

### To connect using Putty from Windows laptop

NOTE: The following instructions are for using Putty. You can also use other popular SSH tools such as [MobaXterm](https://mobaxterm.mobatek.net/) or [SmarTTY](http://smartty.sysprogs.com/)

- You were sent a PEM and a PPK.

- Use Putty to connect to your node using the ppk key:
  - Connection > SSH > Auth > Private key for authentication > Browse... > Select cdf.ppk
![Image](https://raw.githubusercontent.com/horsch37/CDF-Workshop/master/putty.png)

- Create a new session called `cdf-workshop`
   - For the Host Name use: centos@IP_ADDRESS_OF_EC2_NODE
   - Click "Save" on the session page before logging in

![Image](https://github.com/horsch37/CDF-Workshop/raw/master/putty-session.png)

### To connect from Linux/MacOSX laptop

- SSH into your EC2 node using below steps:
- Copy pem key to ~/.ssh dir and correct permissions
    ```
    cp ~/Downloads/cdf.pem ~/.ssh/
    chmod 400 ~/.ssh/cdf.pem
    ```
 - Login to the ec2 node of the you have been assigned by replacing IP_ADDRESS_OF_EC2_NODE below with EC2 node IP Address (your instructor will provide this)
    ```
     ssh -i  ~/.ssh/cdf.pem centos@IP_ADDRESS_OF_EC2_NODE

    ```

  - To change user to root you can:
    ```
    sudo su -
    ```

# Lab 3

  ![Image](https://raw.githubusercontent.com/horsch37/CDF-Workshop/master/cemMiNiFiJavaEdit.png)

## Getting started with MiNiFi and EFM ##

In this lab, we will learn how to configure MiNiFi to send data to NiFi:

* Setting up the Flow for NiFi
* Setting up the Flow for MiNiFi
* Preparing the flow for MiNiFi
* Configuring and starting MiNiFi
* Enjoying the data flow!

As root (sudo su -) install and start EFM, MiNiFi Java

```bash
git clone https://github.com/horsch37/iot-hackathon /opt/demo
yum install -y https://download.postgresql.org/pub/repos/yum/9.6/redhat/rhel-7-x86_64/pgdg-redhat96-9.6-3.noarch.rpm
yum install -y postgresql96-server postgresql96-contrib
/usr/pgsql-9.6/bin/postgresql96-setup initdb
sed -i 's,#port = 5432,port = 5433,g' /var/lib/pgsql/9.6/data/postgresql.conf
sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/g" /var/lib/pgsql/9.6/data/postgresql.conf
sed -i "s/\(127.0.0.1\/32\s\+\)ident/\1trust/g" /var/lib/pgsql/9.6/data/pg_hba.conf 
echo 'host    all          all            0.0.0.0/0  trust' | sudo tee -a /var/lib/pgsql/9.6/data/pg_hba.conf

systemctl enable postgresql-9.6.service
systemctl start postgresql-9.6.service

echo "CREATE DATABASE efm;" | sudo -u postgres psql -U postgres -h localhost -p 5433
echo "CREATE USER efm WITH PASSWORD 'cloudera';" | sudo -u postgres psql -U postgres -h localhost -p 5433
echo "GRANT ALL PRIVILEGES ON DATABASE efm TO efm;" | sudo -u postgres psql -U postgres -h localhost -p 5433

mkdir -p /opt/cloudera/cem
wget http://archive.cloudera.com/CFM/centos7/1.x/updates/1.0.0.0/tars/nifi/nifi-toolkit-1.9.0.1.0.0.0-90-bin.tar.gz -P /opt/cloudera/cem 
wget https://archive.cloudera.com/CEM/centos7/1.x/updates/1.0.0.0/CEM-1.0.0.0-centos7-tars-tarball.tar.gz -P /opt/cloudera/cem
tar xzf /opt/cloudera/cem/CEM-1.0.0.0-centos7-tars-tarball.tar.gz -C /opt/cloudera/cem
tar xzf /opt/cloudera/cem/CEM/centos7/1.0.0.0-54/tars/efm/efm-1.0.0.1.0.0.0-54-bin.tar.gz -C /opt/cloudera/cem
tar xzf /opt/cloudera/cem/CEM/centos7/1.0.0.0-54/tars/minifi/minifi-0.6.0.1.0.0.0-54-bin.tar.gz -C /opt/cloudera/cem
tar xzf /opt/cloudera/cem/CEM/centos7/1.0.0.0-54/tars/minifi/minifi-toolkit-0.6.0.1.0.0.0-54-bin.tar.gz -C /opt/cloudera/cem
tar xzf /opt/cloudera/cem/nifi-toolkit-1.9.0.1.0.0.0-90-bin.tar.gz -C /opt/cloudera/cem
/bin/rm -f /opt/cloudera/cem/CEM-1.0.0.0-centos7-tars-tarball.tar.gz
/bin/rm -f /opt/cloudera/cem/nifi-toolkit-1.9.0.1.0.0.0-90-bin.tar.gz
ln -s /opt/cloudera/cem/efm-1.0.0.1.0.0.0-54 /opt/cloudera/cem/efm
ln -s /opt/cloudera/cem/minifi-0.6.0.1.0.0.0-54 /opt/cloudera/cem/minifi
ln -s /opt/cloudera/cem/efm/bin/efm.sh /etc/init.d/efm

chown -R root:root /opt/cloudera/cem/efm-1.0.0.1.0.0.0-54
chown -R root:root /opt/cloudera/cem/minifi-0.6.0.1.0.0.0-54
chown -R root:root /opt/cloudera/cem/minifi-toolkit-0.6.0.1.0.0.0-54
/bin/rm -f /opt/cloudera/cem/efm/conf/efm.properties
cp /opt/demo/efm.properties /opt/cloudera/cem/efm/conf
/bin/rm -f /opt/cloudera/cem/minifi/conf/bootstrap.conf
cp /opt/demo/bootstrap.conf /opt/cloudera/cem/minifi/conf
sed -i "s/YourHostname/demo.hortonworks.com/g" /opt/cloudera/cem/efm/conf/efm.properties
sed -i "s/YourHostname/demo.hortonworks.com/g" /opt/cloudera/cem/minifi/conf/bootstrap.conf
/opt/cloudera/cem/minifi/bin/minifi.sh install

service minifi start
service efm start
```

Visit [EFM UI](http://YOURIP:10080/efm/ui/)

![EFM UI](https://raw.githubusercontent.com/horsch37/CDF-Workshop/master/cemMiNiFiJavaEdit.png)

You should see heartbeats coming from the agent.

![EFM agents monitor](https://raw.githubusercontent.com/horsch37/CDF-Workshop/master/cemEvents2.png)

Now, **on the root canvas**, create a simple flow to collect local syslog messages and forward them to NiFi, where the logs will be parsed, transformed into another format and pushed to a Kafka topic.

Our Java MiNiFi Agent has been tagged with the class 'iot-1' (check nifi.c2.agent.class property in minifi-0.6.0.1.0.0.0-54/conf/bootstrap.conf)

Now we should be ready to create our flow in Nifi. To do this do the following:

1.	The first thing we are going to do is setup an Input Port. This is the port that MiNiFi will be sending data to. To do this drag the Input Port icon to the canvas and call it whatever you like.  We are only interested in the id.

![Syslog message](https://raw.githubusercontent.com/horsch37/CDF-Workshop/master/jimsyslognifi.png)

2. Now that the Input Port is configured we need to have somewhere for the data to go once we receive it. In this case we will publish the message to Kafka using a GrokReader and JSONWriter. To do this drag the Processor icon to the canvas and choose the PublishKafkaRecord_2_0 processor.

![Kafka Write](https://raw.githubusercontent.com/horsch37/CDF-Workshop/master/jimkafkawrite.png)
![GROK Settings](https://raw.githubusercontent.com/horsch37/CDF-Workshop/master/jimgrok.png)

We are going to use a Grok parser to parse the Syslog messages. Here is a Grok expression that can be used to parse such logs format:

```%{SYSLOGTIMESTAMP:syslog_timestamp} %{SYSLOGHOST:syslog_hostname} %{DATA:syslog_program}(?:\[%{POSINT:syslog_pid}\])?: %{GREEDYDATA:syslog_message}```

Now that we have built the Apache NiFi flow that will receive the logs, let's setup the nifi registry then go back to the EFM UI and build the MiNiFi flow.

Go to NiFi Registry and create a bucket named **IoT**

![NiFi Registry](https://raw.githubusercontent.com/tspannhw/CDF-Workshop/master/nifiregistry.png)

Build the MiNiFi flow:
![CEM_Flow](https://raw.githubusercontent.com/horsch37/CDF-Workshop/master/cemMiNiFiJavaEdit.png)

This MiNiFi agent will tail /var/log/messages or /var/log/secure and send the logs to a remote process group (our NiFi instance) using the Input Port.

Check to make sure only 1 concurrent call.

Please note that the NiFi instance has been configured to receive data over HTTP only, not RAW!

![Remote process group](https://raw.githubusercontent.com/tspannhw/CDF-Workshop/master/configureRemoteProcessGroup.png)

Now we can start the NiFi flow and publish the MiNiFi flow to NiFi registry (Actions > Publish...)

Visit [NiFi Registry UI](http://YOURURL:61080/nifi-registry/explorer/grid-list) to make sure your flow has been published successfully.

Within few seconds, you should be able to see syslog messages streaming through your NiFi flow and be published to the Kafka topic you have created.

![EFM REST API](https://raw.githubusercontent.com/tspannhw/CDF-Workshop/master/efmapi.png)

You may tail the log of the MiNiFi application by
   ```
   tail -f (See Log Directories) logs/minifi-app.log 
   ```
If you see error logs such as "the remote instance indicates that the port is not in a valid state",
it is because the Input Port has not been started.
Start the port and you will see messages being accumulated in its downstream queue.

![Image](https://github.com/jhorsch/CDF-Workshop/raw/master/nifiportdetails2.png)

------------------

# Lab 4

## Kafka Basics
In this lab we are going to explore creating, writing to and consuming Kafka topics. This will come in handy when we later integrate Kafka with NiFi and Streaming Analytics Manager.  See:  https://kafka.apache.org/quickstart

1. Creating a topic
  - Step 1: Open an SSH connection to your EC2 Node.
  - Step 2: Naviagte to the Kafka directory (````/usr/hdf/current/kafka-broker````), this is where Kafka is installed, we will use the utilities located in the bin directory.

    ````
    cd /usr/hdf/current/kafka-broker/
    ````

  - Step 3: Create a topic using the kafka-topics.sh script
    ````
    bin/kafka-topics.sh --create --zookeeper demo.hortonworks.com:2181 --replication-factor 1 --partitions 1 --topic meetup_rsvp_raw
    ````

    NOTE: Based on how Kafka reports metrics topics with a period ('.') or underscore ('_') may collide with metric names and should be avoided. If they cannot be avoided, then you should only use one of them.

  - Step 4:	Ensure the topic was created
    ````
    bin/kafka-topics.sh --list --zookeeper demo.hortonworks.com:2181
    ````

2. Testing Producers and Consumers
  - Step 1: Open a second terminal to your EC2 node and navigate to the Kafka directory
  - In one shell window connect a consumer:
    ````
    bin/kafka-console-consumer.sh --bootstrap-server demo.hortonworks.com:6667 --topic meetup_rsvp_raw --from-beginning
    ````

    Note: using –from-beginning will tell the broker we want to consume from the first message in the topic. Otherwise it will be from the latest offset.

  - In the second shell window connect a producer:
````
bin/kafka-console-producer.sh --broker-list demo.hortonworks.com:6667 --topic meetup_rsvp_raw
````
- Sending messages. Now that the producer is  connected  we can type messages.
  - Type a message in the producer window

- Messages should appear in the consumer window.

- Close the consumer (Ctrl-C) and reconnect using the default offset, of latest. You will now see only new messages typed in the producer window.

- As you type messages in the producer window they should appear in the consumer window.

Make sure all the messages are consumed from the topic before closing as we will use this topic for your integration example.

------------------

# Lab 5

## Integrating Kafka with NiFi

Using the topic already created, meetup_rsvp_raw, we will publish from Apache NiFi.

1. Integrating NiFi
  - Step 1: Add a PublishKafka_*_0 processor to the canvas.
  - Step 2: Add a routing for the success relationship of the ReplaceText processor to the PublishKafka_*_0 processor added in Step 1 as shown below:
  
    ![Image](https://github.com/tspannhw/CDF-Workshop/raw/master/publishkafka.png)
    
  - Step 3: Configure the topic and broker for the PublishKafka_*_0 processor,
  where topic is meetup_rsvp_raw and broker is localhost:6667.

3. Start the NiFi flow
4. In a terminal window to your EC2 node and navigate to the Kafka directory and connect a consumer to the ````meetup_rsvp_raw```` topic:

    ````
    bin/kafka-console-consumer.sh --bootstrap-server demo.hortonworks.com:6667 --topic meetup_rsvp_raw --from-beginning
    ````

5. Messages should now appear in the consumer window.


------------------

# Lab 6

## Integrating the Schema Registry
1. Creating the topic
  - Step 1: Open an SSH connection to your EC2 Node.
  - Step 2: Naviagte to the Kafka directory (````/usr/hdf/current/kafka-broker````), this is where Kafka is installed, we will use the utilities located in the bin directory.

    ````
    cd /usr/hdf/current/kafka-broker/
    ````

  - Step 3: Create a topic using the kafka-topics.sh script
    ````
    bin/kafka-topics.sh --create --zookeeper demo.hortonworks.com:2181 --replication-factor 1 --partitions 1 --topic meetup_rsvp_avro

    ````

    NOTE: Based on how Kafka reports metrics topics with a period ('.') or underscore ('_') may collide with metric names and should be avoided. If they cannot be avoided, then you should only use one of them.

  - Step 4:	Ensure the topic was created
    ````
    bin/kafka-topics.sh --list --zookeeper demo.hortonworks.com:2181
    ````

2. Adding the Schema to the Schema Registry
  - Step 1: Open a browser and navigate to the Schema Registry UI. You can get to this from the either the ```Quick Links``` drop down in Ambari, as shown below:

    ![Image](https://github.com/tspannhw/CDF-Workshop/raw/master/ambarisr.png)
    ![Image](https://github.com/tspannhw/CDF-Workshop/raw/master/schemaregistry2.png)
    ![Image](https://github.com/tspannhw/CDF-Workshop/raw/master/srAddSchema.png)
    ![Image](https://github.com/tspannhw/CDF-Workshop/raw/master/srcontrollerconfiguration.png)

    ![Image](https://github.com/tspannhw/CDF-Workshop/raw/master/registry_quick_link.png)

    or by going to ````http://<EC2_NODE>:7788/ui/#/
    
  - Step 2: Create Meetup RSVP Schema in the Schema Registry
    1. Click on “+” button to add new schemas. A window called “Add New Schema” will appear.
    2. Fill in the fields of the ````Add Schema Dialog```` as follows:

        ![Image](https://github.com/tspannhw/CDF-Workshop/raw/master/add_schema_dialog.png)

        For the Schema Text you can download it [here](https://raw.githubusercontent.com/tspannhw/CDF-Workshop/master/meetup_rsvp.asvc) and either copy and paste it or upload the file.

        Once the schema information fields have been filled and schema uploaded, click **Save**.

3. We are now ready to integrate the schema with NiFi

  - Step 0: Remove the PutFile and PublishKafka* processors from the canvas, we will not need them for this section.
  - Step 1: Add a UpdateAttribute processor to the canvas.
  - Step 2: Add a routing for the success relationship of the ReplaceText processor to the UpdateAttrbute processor added in Step 1.
  - Step 3: Configure the UpdateAttribute processor as shown below:

    ![Image](https://github.com/tspannhw/CDF-Workshop/raw/master/update_attribute_schema_name.png)

  - Step 4: Add a JoltTransformJSON processor to the canvas.
  - Step 5: Add a routing for the success relationship of the UpdateAttribute processor to the JoltTransformJSON processor added in Step 5.
  - Step 6: Configure the JoltTransformJSON processor as shown below:

    ![Image](https://github.com/tspannhw/CDF-Workshop/raw/master/jolt_transform_config.png)

    The JSON used in the 'Jolt Specification' property is as follows:

    ``
    {
      "venue": {
        "lat": ["=toDouble", 0.0],
        "lon": ["=toDouble", 0.0]
      }
    }
  ``
  - Step 7: Add a LogAttribute processor to the canvas.
  - Step 8: Add a routing for the failure relationship of the JoltTransformJSON processor to the LogAttribute processor added in Step 7.
  - Step 9: Add a PublishKafkaRecord_*_0 to the canvas.
  - Step 10: Add a routing for the success relationship of the JoltTransformJSON processor to the PublishKafkaRecord_1_0 processor added in Step 9.
  - Step 11: Configure the PublishKafkaRecord_*_0 processor to look like the following:

    ![Image](https://github.com/tspannhw/CDF-Workshop/raw/master/publishkafka_record_configuration.png)


  - Step 12: When you configure the JsonTreeReader and AvroRecordSetWriter, you will first need to configure a schema registry controller service. The schema registry controller service we are going to use is the 'HWX Schema Registry', it should be configured as shown below:

    ![Image](https://github.com/tspannhw/CDF-Workshop/raw/master/hwx_schema_registry_config.png)

  - Step 13: Configure the JsonTreeReader as shown below:

    ![Image](https://github.com/tspannhw/CDF-Workshop/raw/master/json_tree_reader_config.png)

  - Step 14: Configure the AvroRecordSetWriter as shown below:

      ![Image](https://github.com/tspannhw/CDF-Workshop/raw/master/avro_recordset_writer.png)

    After following the above steps this section of your flow should look like the following:

    ![Image](https://github.com/tspannhw/CDF-Workshop/raw/master/update_jolt_kafka_section.png)


4. Start the NiFi flow
5. In a terminal window to your EC2 node and navigate to the Kafka directory and connect a consumer to the ````meetup_rsvp_avro```` topic:

    ````
    bin/kafka-console-consumer.sh --bootstrap-server demo.hortonworks.com:6667 --topic meetup_rsvp_avro --from-beginning
    ````

5. Messages should now appear in the consumer window.
