from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("customer_id") == 1) & (~F.col("country").isin(["UK", "SG"]) | (F.col("device") != "tablet")))
    .select("customer_id", "browser", F.lower("device").alias("device_lower"))
)
