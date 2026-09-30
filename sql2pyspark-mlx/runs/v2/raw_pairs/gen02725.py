from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country").isNull() & F.col("country").isin(["JP", "TH"])) | F.col("customer_id").isNotNull())
    .withColumn("device_prefix", F.substring("device", 1, 2))
    .withColumn("country_title", F.initcap(F.trim("country")))
)
