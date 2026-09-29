from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser").isin(["Safari", "Firefox", "Chrome"]) & F.col("device").isin(["tablet", "mobile"])) | (F.col("customer_id") == 10))
    .withColumn("browser_device_key", F.concat_ws("/", "browser", "device"))
    .withColumn("started_at_date", F.to_date("started_at"))
)
