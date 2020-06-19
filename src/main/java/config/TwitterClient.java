package config;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterClient {
    final static Logger log= LoggerFactory.getLogger(TwitterClient.class.getName());

    static BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
    public static void main(String[] args) throws IOException {
        final TwitterClient client = new TwitterClient();
        Client clnt=client.buildTwitterClient();
        clnt.connect();

        Thread consumer = new Thread((()->{
            System.out.println(clnt.getName());
            while (!clnt.isDone()) {
                try {
                    String msg = msgQueue.take();
                    System.out.println(msg);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }));

        consumer.start();
    }

    public Client buildTwitterClient() throws IOException {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("keyWord1", "keyWord");
        hosebirdEndpoint.trackTerms(terms);

        Properties properties= new Properties();
        InputStream strm = this.getClass().getClassLoader().getResourceAsStream("application-bkp.properties");
        properties.load(strm);

        Authentication hosebirdAuth = new OAuth1(properties.get("api-key").toString(), properties.get("api-secret").toString(),
                properties.get("access-token").toString() , properties.get("access-secret").toString());

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();

    }

}
