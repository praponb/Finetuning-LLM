from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "browser", "customer_id", F.upper("device").alias("device_upper"))
)
