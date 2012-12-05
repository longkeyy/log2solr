import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class LogToSolr {
	private static String SolrAddress = "http://localhost:8080/solr/syslog";
	private static String[] fieldName = { "id", "facility", "severity",
			"timestamp", "hostname", "msg" };
	private final static String[] Facility = { "kernel messages ",
			"user-level messages ", "mail system ", "system daemons ",
			"security/authorization messages ",
			"messages generated internally by syslogd ",
			"line printer subsystem ", "network news subsystem ",
			"UUCP subsystem ", "clock daemon ",
			"security/authorization messages ", "FTP daemon ",
			"NTP subsystem ", "log audit ", "log alert ", "clock daemon ",
			"local0", "local1", "local2", "local3", "local4", "local5",
			"local6", "local7" };
	private final static String[] Severity = { "Emergency", "Alert",
			"Critical", "Error", "Warning", "Notice", "Informational", "Debug" };
	private static String Properties_FILE = System.getProperty("user.dir")
			+ "/Log2solr.properties";
	// private static final int nThreads = 50;
	//
	// private static ExecutorService es =
	// Executors.newFixedThreadPool(nThreads);
	private static SolrServer server;

	private static void init() {
		File x = new File(Properties_FILE);
		if (!x.exists())
			return;
		String fieldNames = "id,facility,severity,timestamp,hostname,msg";
		StringBuffer sb = new StringBuffer(1000);
		for (int i = 0, len = fieldName.length; i < len; i++) {
			sb.append(fieldName[i]);
		}
		System.out.println(sb);
		Properties props = new Properties();

		try {
			BufferedInputStream bufferdInputStream = new BufferedInputStream(
					new FileInputStream(Properties_FILE));
			try {
				props.load(bufferdInputStream);
				SolrAddress = props.getProperty("SolrAddress", SolrAddress);
				fieldNames = props.getProperty("fieldNames", fieldNames);
				fieldName = fieldNames.split(",");
				bufferdInputStream.close();
				System.out.println(SolrAddress);
				// System.out.println(fieldNames);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void addIndex(String[] fieldArr, String[] valueArr) {
		// String[] fieldName = fieldNames.split(",");
		if (fieldArr.length != valueArr.length)
			return;
		try {
			SolrInputDocument doc = new SolrInputDocument();
			for (int i = 0, len = fieldArr.length; i < len; i++) {
				doc.addField(fieldArr[i], valueArr[i]);
			}
			server.add(doc);
			// server.commit();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static String[] docFilterSyslog(String[] fieldName, String s) {

		// String[] fieldName = fieldNames.split(",");
		Pattern pattern = Pattern
				.compile("<(.+)>\\d\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(.*)");
		Matcher matcher = pattern.matcher(s);
		if (matcher.matches()) {
			String[] Value = new String[9];
			int m = new Integer(matcher.group(1));
			Value[0] = Long.toString(System.currentTimeMillis()
					+ (new Random()).nextInt(1000));
			Value[1] = Severity[m % 8];
			Value[2] = Facility[m >> 3]; // l/8
			Value[3] = Long.toString(System.currentTimeMillis());
			Value[4] = matcher.group(2);
			Value[5] = matcher.group(8);
			return Value;
		} else {

		}

		String[] Value = new String[9];
		String[] v = s.split(" ", 9);
		if (v.length < 9) {
			return Value;
		}

		Value[0] = Long.toString(System.currentTimeMillis()
				+ (new Random()).nextInt(1000));

		Value[3] = Long.toString(System.currentTimeMillis());
		Value[4] = v[2];
		Value[5] = v[8];

		Pattern p = Pattern.compile("<(.+)>\\d");
		Matcher m = p.matcher(v[0]);
		if (m.find()) {
			int l = new Integer(m.group(1));
			Value[1] = Severity[l % 8];
			Value[2] = Facility[l >> 3]; // l/8
		}

		return Value;
	}

	public static void main(String[] args) throws MalformedURLException,
			UnsupportedEncodingException {
		server = new CommonsHttpSolrServer(SolrAddress);
		init();
		String s = "";
		String[] doc;

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		try {
			while ((s = br.readLine()) != null) {
				// System.out.println(s);
				doc = docFilterSyslog(fieldName, s);
				if (doc[0] != null) {
					addIndex(fieldName, doc);
				}
				// es.submit(new LogInsert(s));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	static class LogInsert implements Callable<String> {

		private String string;
		private Pattern pattern = Pattern
				.compile("<(.+)>\\d\\s+(\\S+)\\s+(\\S+)\\s+(.*)");
		private static final int[] groupArr = new int[] { 1, 3, 4 };
		private static final String[] fieldArr = new String[] { "level",
				"host", "msg" };
		private Random rnd = new Random();

		public LogInsert(String string) {
			super();
			this.string = string;
		}

		@Override
		public String call() throws Exception {

			Matcher matcher = pattern.matcher(string);
			if (matcher.find()) {
				SolrInputDocument document = new SolrInputDocument();
				document.addField("id",
						System.currentTimeMillis() + rnd.nextInt(1000));
				for (int i = 0; i < groupArr.length; i++) {
					document.addField(fieldArr[i], matcher.group(groupArr[i]));
				}
				server.add(document);
				// server.commit();

			} else {
				// log.println("miss: " + s);
			}
			// log.flush();
			return null;
		}

	}

}