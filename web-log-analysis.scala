import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.expressions.Window
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// File location and type
val file_location = "./data/2015_07_22_mktplace_shop_web_log_sample.log.gz"


// The applied options are for CSV files. For other file types, these will be ignored.
val rawDF = spark.read.textFile(file_location).na.drop()
val pattern= "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$"

val cleanDF = rawDF.select(
    regexp_extract(col("value"),pattern, 1) as "timestamp",
    regexp_extract(col("value"),pattern, 2) as "elb",
    regexp_extract(col("value"),pattern, 3) as "client",
    regexp_extract(col("value"),pattern, 4) as "backend",
    regexp_extract(col("value"),pattern, 5) as "req_process_time",
    regexp_extract(col("value"),pattern, 6) as "back_process_time",
    regexp_extract(col("value"),pattern, 7) as "res_process_time",
    regexp_extract(col("value"),pattern, 8) as "elb_status_code",
    regexp_extract(col("value"),pattern, 9) as "back_status_cd",
    regexp_extract(col("value"),pattern, 10) as "recieved_bytes",
    regexp_extract(col("value"),pattern, 11) as "sent_bytes",
    regexp_extract(col("value"),pattern, 12) as "req",
    regexp_extract(col("value"),pattern, 13) as "user_agent",
    regexp_extract(col("value"),pattern, 14) as "ssl_cipher",
    regexp_extract(col("value"),pattern, 15) as "ssl_protocol",
).na.drop()

val client_req = cleanDF.withColumn("date_time", $"timestamp".cast(TimestampType))
      .sort($"timestamp")
      .select($"client",$"req",$"date_time").na.drop()

client_req.createOrReplaceTempView("client_req")

// 1. Sessionize the data
val window_of_cleint_ip = Window.partitionBy($"client").orderBy($"date_time")

val lag_timestamp = client_req.withColumn("lag_date_time", lag($"date_time", 1).over(window_of_cleint_ip))

val web_sessions = lag_timestamp
  .withColumn("session_flag", when((unix_timestamp($"date_time") - unix_timestamp($"lag_date_time") >= (60*15)) or isnull($"lag_date_time"), 1 )
  otherwise 0)

val session_id = web_sessions.withColumn("session_id",sum($"session_flag").over(window_of_cleint_ip))

session_id.show()

// 2. average session time
val session_tm_win = session_id.groupBy($"client", $"session_id")
        .agg((unix_timestamp(max($"date_time")) - unix_timestamp(min($"date_time"))).as("session_time"))
        .agg(avg($"session_time").as("avg_session_time"))

session_tm_win.show()
    
// 3. unique url visits per session
val unq_url_visits = session_id.groupBy($"client", $"session_id")
    .agg(countDistinct($"req").as("distinct_url"))

unq_url_visits.show()

// 4. Top 10  most engaged user
val engaged_users = session_id.groupBy($"client",$"session_id").agg((unix_timestamp(max($"date_time")) - unix_timestamp(min($"date_time"))).as("session_time"))

val most_engaged_users = engaged_users.groupBy($"client").agg(avg($"session_time")
    .as("avg_session_time"))
    .orderBy($"avg_session_time".desc)

most_engaged_users.show()
