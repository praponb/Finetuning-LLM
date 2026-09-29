from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "tablet") & F.col("customer_id").isNull())
    .withColumn("device_prefix", F.substring("device", 1, 3))
)
