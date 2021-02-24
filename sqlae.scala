// Databricks notebook source
Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

// COMMAND ----------

// Databricks notebook source
// DBTITLE 1,Import Microsoft ADAL Library
// REQUIREMENT - The com.microsoft.aad.adal4j artifact must be included as a dependency.
import com.microsoft.aad.msal4j.{ConfidentialClientApplication,ClientCredentialFactory,ClientCredentialParameters}
import java.util.Collections
//import com.microsoft.sqlserver.jdbc.SQLServerDriver.SQLServerColumnEncryptionAzureKeyVaultProvider

// COMMAND ----------

// DBTITLE 1,Import Dependencies
import org.apache.spark.sql.SparkSession
import java.util.concurrent.Executors

// COMMAND ----------

// DBTITLE 1,Setup Connection Properties
val jdbcurl = "jdbc:sqlserver://-.database.windows.net:1433;databaseName=-"
val dbTable = "dbo.Patients"

// In the below example, we're using a Databricks utility that facilitates acquiring secrets from 
// a configured Key Vault.  

// Service Principal Client ID - Created in App Registrations from Azure Portal
val principalClientId = dbutils.secrets.get(scope="aescope",key="principalid")

// Service Principal Secret - Created in App Registrations from Azure Portal
val principalSecret = dbutils.secrets.get(scope="aescope",key="secret")

// Located in App Registrations from Azure Portal
val TenantId = "-"

val authority = "https://login.windows.net/" + TenantId
val resourceAppIdURI = "https://database.windows.net/.default"

val jdbcPort = 1433
val jdbcDatabase = "<database>"



// COMMAND ----------

// DBTITLE 1,Authentication Options
// SERVICE PRINCIPAL AUTHENTICATION
//   You will need to obtain an access token.
//   The "accessToken" option is used in the spark dataframe to indicate this 
//   authentication modality.

val app = ConfidentialClientApplication.builder(
                   principalClientId,
                   ClientCredentialFactory.createFromSecret(principalSecret))
                   .authority(authority)
                   .build();

val clientCredentialParam = ClientCredentialParameters.builder(
   Collections.singleton(resourceAppIdURI))
   .build();

val future = app.acquireToken(clientCredentialParam);
val token = future.get().accessToken()


// ACTIVE DIRECTORY PASSWORD AUTHENTICATION
//   The "authentication" option with the value of "ActiveDirectoryPassword"
//   is used in the spark dataframe to indicate this 
//   authentication modality. 
//
//   The "user" and "password" options apply to both SQL Authentication
//   and Active Directory Authentication.  SQL Authentication is used
//   by default and can be switched to Active Directory with the
//   authentication option above.
//val user = dbutils.secrets.get("adUser")
//val password = dbutils.secrets.get("adPassword")


// COMMAND ----------

// DBTITLE 1,Query SQL using Spark with Service Principal

//val akvProvider = new SQLServerColumnEncryptionAzureKeyVaultProvider(clientID, clientKey);
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.setProperty("Driver", driverClass)
connectionProperties.put("dbtable", "dbo.Patients")
connectionProperties.put("accessToken", s"${token}")
connectionProperties.put("encrypt", "true")
connectionProperties.put("columnEncryptionSetting","enabled")
//connectionProperties.put("keyStoreAuthentication","KeyVaultClientSecret")
connectionProperties.put("keyVaultProviderClientId",principalClientId)
connectionProperties.put("keyVaultProviderClientKey", principalSecret)
connectionProperties.put("hostNameInCertificate","*.database.windows.net")

//print(driverClass.getDriverName())

val jdbcDF = spark.read
    .jdbc(jdbcurl,"dbo.Patients",connectionProperties)

jdbcDF.printSchema

display(jdbcDF.select("SSN","LastName"))
         
