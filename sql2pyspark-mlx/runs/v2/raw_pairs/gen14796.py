from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") > 0.35) & (F.col("customer_id") == 2))
    .select("id", "store_id", "status", F.length("channel").alias("channel_length"))
    .orderBy("status", F.col("id").desc())
)
