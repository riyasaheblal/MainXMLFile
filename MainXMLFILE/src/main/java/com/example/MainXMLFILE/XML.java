package com.example.MainXMLFILE;

import org.w3c.dom.*;

import com.example.MainXMLFILE.util.CommonConstant;

import javax.xml.parsers.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;

@Component
@PropertySource("classpath:application.properties")

public class XML {
	@Value("${request.directory.path}")
	public static String requestDirectoryPath;

	@Value("${response.directory.path}")
	public static String responseDirectoryPath;

	@Value("${res.success.path}")
	private static String resSuccessPath;

	@Value("${res.failed.path}")
	private static String resFailedPath;

	@Value("${req.success.path}")
	private static String reqSuccessPath;

	@Value("${req.failed.path}")
	private static String reqFailedPath;

	@Value("${db.url}")
	private static String url;

	@Value("${db.username}")
	private static String username;

	@Value("${db.password}")
	private static String password;

	@Value("${sql.insertProcedure}")
	private static String insertProcedureSql;

	@Value("${sql.updateProcedure}")
	private static String updateProcedureSql;





	public static void main(String[] args) {
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

		executor.scheduleWithFixedDelay(() -> {
			ArrayList<String> dataList = new ArrayList<>();
			try {
				parseAndInsertXMLFiles(requestDirectoryPath, reqSuccessPath, reqFailedPath);}
			catch (Exception e) {
				e.printStackTrace();			}
			updateXMLFiles(responseDirectoryPath, dataList, resSuccessPath, resFailedPath);
		}, 0, 1, TimeUnit.MINUTES); 

		Runtime.getRuntime().addShutdownHook(new Thread(executor::shutdown));
	}



	private static void parseAndInsertXMLFiles(String directoryPath, String reqSuccessPath, String reqFailedPath) {
		File folder = new File(directoryPath);
		if (folder.isDirectory()) {
			File[] files = folder.listFiles();
			if (files != null) {
				for (File xmlFile : files) {
					if (xmlFile.isFile() && xmlFile.getName().toLowerCase().endsWith(".xml")) {
						boolean success = parseXML(xmlFile.getPath());
						if (success) {
							moveFile(xmlFile, reqSuccessPath);
						} else {
							moveFile(xmlFile, reqFailedPath);
						}
					} else {
						System.out.println("This file is already in read format: " + xmlFile.getName() );
					}
				}
			}
		} else {
			System.out.println( "Provided path is not a directory." );
		}
	}

	private static void updateXMLFiles(String directoryPath, ArrayList<String> dataList, String resSuccessPath, String resFailedPath) {
		File folder = new File(directoryPath);
		if (folder.isDirectory()) {
			File[] files = folder.listFiles();
			if (files != null) {
				for (File xmlFile : files) {
					if (xmlFile.isFile() && xmlFile.getName().toLowerCase().endsWith(".xml")) {
						boolean success = processXMLFile(xmlFile, dataList);
						if (success) {
							moveFile(xmlFile, resSuccessPath);
						} else {
							moveFile(xmlFile, resFailedPath);
						}
					} else {
						System.out.println( "This file cannot be updated: " + xmlFile.getName() );
					}
				}
			}
		} else {
			System.out.println("Provided path is not a directory." );
		}
	}

	private static void moveFile(File file, String destinationPath) {
		Path source = file.toPath();
		String fileName = file.getName();
		String timestamp = String.valueOf(System.currentTimeMillis());
		String newFileName = fileName.substring(0, fileName.lastIndexOf('.')) + "_" + timestamp + fileName.substring(fileName.lastIndexOf('.'));
		Path destination = Paths.get(destinationPath, newFileName);
		try {
			Files.move(source, destination);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Connection getDBConnection() throws Exception {
		return	DriverManager.getConnection(url, username, password);

	}

	public static boolean  parseXML(String filePath) {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			File file = new File(filePath);
			Document document = builder.parse(file);
			document.getDocumentElement().normalize();

			Element root = document.getDocumentElement();
			String recordsCount = root.getAttribute("RecordsCount");
			String bankName = root.getAttribute("BankName");
			String bankCode = root.getAttribute("BankCode");
			String destination = root.getAttribute("Destination");
			String source = root.getAttribute("Source");
			String messageId = root.getAttribute("MessageId");

			List<String> accountList = new ArrayList<>();
			NodeList accountNodeList = root.getElementsByTagName("Account");

			for (int i = 0; i < accountNodeList.getLength(); i++) {
				Node accountNode = accountNodeList.item(i);
				if (accountNode.getNodeType() == Node.ELEMENT_NODE) {
					Element accountElement = (Element) accountNode;
					String accountNumber = accountElement.getElementsByTagName("AccountNumber").item(0).getTextContent();
					String entityCode = accountElement.getElementsByTagName("EntityCode").item(0).getTextContent();
					String dataRequired	 = accountElement.getElementsByTagName("DataRequired").item(0).getTextContent();

					String concatenatedDetails = messageId + "|" + accountNumber + "|" + source + "|" + destination + "|"
							+ bankCode + "|" + bankName  + "|"+ recordsCount  + "|"+ entityCode + "|"+ dataRequired  ;	

					accountList.add(concatenatedDetails);
				}
			}
			int accountsRead = accountList.size();
			int accountsInserted = 0;
			Connection connection = getDBConnection();
			CallableStatement callableStatement = connection.prepareCall(insertProcedureSql);
			for (String concatenatedAccount : accountList) {
				callableStatement.setString(1, concatenatedAccount);
				callableStatement.execute();
				accountsInserted++;
			}
			if (accountsRead != accountsInserted) {
				System.out.println("Insertion count does not match the read count!");
			}
			else {
				System.out.println("Sucessfully inserted the data and the count are same of 'read' file is "+"'"+ accountsRead+"'"+" and the 'inserted' file is "+ "'"+accountsInserted+"'");
			}
			callableStatement.close();
			connection.close();           
			for (String concatenatedAccount : accountList) {
				System.out.println(concatenatedAccount);
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}


	private static boolean processXMLFile(File xmlFile, ArrayList<String> dataList) {
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(xmlFile);
			doc.getDocumentElement().normalize();
			NodeList accountsList = doc.getElementsByTagName("Accounts");

			for (int temp = 0; temp < accountsList.getLength(); temp++) {
				Element accountsElement = (Element) accountsList.item(temp);
				String accountsData = extractAccountsData(accountsElement);
				NodeList accountList = doc.getElementsByTagName("Account");
				for (int i = 0; i < accountList.getLength(); i++) {
					Element accountElement = (Element) accountList.item(i);
					String ahDetailsData = extractAHDetailsData(accountElement);
					String concat=accountsData + "|" + ahDetailsData;
					dataList.add(concat);
				}
			}
			int ResponseRead = dataList.size();
			int accountsUpdated = 0;
			Connection connection = getDBConnection();
			CallableStatement callableStatement = connection.prepareCall(updateProcedureSql);

			for (String concatenatedAccount : dataList) {
				callableStatement.setString(1, concatenatedAccount);
				callableStatement.execute();
				accountsUpdated++;
			}
			if (ResponseRead != accountsUpdated) {
				System.out.println("Insertion count does not match the read count!");
			}
			else {
				System.out.println("Read and update count of the data. Read count: " + ResponseRead + ", Inserted count: " + accountsUpdated);
			}
			callableStatement.close();
			connection.close();
			for (String concatenatedAccount : dataList) {
				System.out.println(concatenatedAccount);
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	private static String extractAccountsData(Element element) {
		return element.getAttribute("MessageId");
	}

	private static String extractAHDetailsData(Element element) {
		Element ahElement = (Element) element.getElementsByTagName("AH").item(0);

		return element.getElementsByTagName("ReqMsgId").item(0).getTextContent() + "|" +
		element.getElementsByTagName("AccountNumber").item(0).getTextContent() + "|" +
		element.getElementsByTagName("AccountValidity").item(0).getTextContent() + "|" +
		element.getElementsByTagName("AccountStatus").item(0).getTextContent() + "|" +
		element.getElementsByTagName("AccountType").item(0).getTextContent() + "|" +
		element.getElementsByTagName("BSRCode").item(0).getTextContent() + "|" +
		element.getElementsByTagName("IFSCCode").item(0).getTextContent() + "|" +
		element.getElementsByTagName("AccountOpenDate").item(0).getTextContent() + "|" +
		element.getElementsByTagName("AccountCloseDate").item(0).getTextContent() + " |" +
		ahElement.getAttribute("emailID") + "|" +
		ahElement.getAttribute("TAN") + "|" +
		ahElement.getAttribute("StateLGDCode") + "|" +
		ahElement.getAttribute("State") + "|" +
		ahElement.getAttribute("PinCode") + "|" +
		ahElement.getAttribute("PAN") + "|" +
		ahElement.getAttribute("Nationality") + "|" +
		ahElement.getAttribute("Name") + "|" +
		ahElement.getAttribute("Mobile") + "|" +
		ahElement.getAttribute("Gender") + "|" +
		ahElement.getAttribute("DistrictLGDCode") + "|" +
		ahElement.getAttribute("District") + "|" +
		ahElement.getAttribute("DOB") + "|" +
		ahElement.getAttribute("AddressLine2") + "|" +
		ahElement.getAttribute("AddressLine1") + "|" +
		ahElement.getAttribute("AHTYPE")  ;
	}

}
