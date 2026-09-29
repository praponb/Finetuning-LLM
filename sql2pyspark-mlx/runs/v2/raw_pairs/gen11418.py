from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & F.col("browser").isin(["Firefox", "Chrome"]))
    .select("id", "customer_id", "device", F.length("device").alias("device_length"))
)
