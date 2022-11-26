package info.unterrainer.htl.htlgrpcclient;

import java.util.concurrent.TimeUnit;


import info.unterrainer.htl.htlgrpcclient.protos.JsonataGrpc;
import info.unterrainer.htl.htlgrpcclient.protos.QueryData;
import info.unterrainer.htl.htlgrpcclient.protos.QueryResult;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GRpcClient {

	private final JsonataGrpc.JsonataBlockingStub blockingStub;

	/**
	 * Construct client for accessing HelloWorld server using the existing channel.
	 */
	public GRpcClient(Channel channel) {
		// 'channel' here is a Channel, not a ManagedChannel, so it is not this code's
		// responsibility to
		// shut it down.

		// Passing Channels to code makes code easier to test and makes it easier to
		// reuse Channels.
		blockingStub = JsonataGrpc.newBlockingStub(channel);
	}

	/** Say hello to server. */
	public void query(String query, String json) {
		log.info("Will try to send query [" + query + "] with data [" + json + "] ...");
		QueryData request = QueryData.newBuilder().setQuery(query).setJson(json).build();
		QueryResult response;
		try {
			response = blockingStub.evaluate(request);
		} catch (StatusRuntimeException e) {
			log.warn("RPC failed: {0}", e);
			return;
		}
		log.info("Answer was: " + response.getValue());
	}

	/**
	 * Greet server. If provided, the first element of {@code args} is the name to
	 * use in the greeting. The second argument is the target server.
	 */
	public static void main(String[] args) throws Exception {
		// Access a service running on the local machine on port 50051
		// If you'd like to change that, use a string like
		// 'localhost:50051' for example.
		String target = "jsonata-grpc.unterrainer.info:50051";

		// Create a communication channel to the server, known as a Channel. Channels
		// are thread-safe
		// and reusable. It is common to create channels at the beginning of your
		// application and reuse
		// them until the application shuts down.
		//
		// For the example we use plaintext insecure credentials to avoid needing TLS
		// certificates. To
		// use TLS, use TlsChannelCredentials instead.
		ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
		try {
			GRpcClient client = new GRpcClient(channel);
			client.query("$sum(elements.price)>2", "{\"elements\":[{\"price\":13},{\"price\":1}]}");
		} finally {
			// ManagedChannels use resources like threads and TCP connections. To prevent
			// leaking these
			// resources the channel should be shut down when it will no longer be used. If
			// it may be used
			// again leave it running.
			channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
		}
	}
}