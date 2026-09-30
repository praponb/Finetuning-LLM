from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([11, 15, 32, 40]) & (F.col("store_id") == 8))
    .withColumn("channel_status_key", F.concat_ws(",", "channel", "status"))
    .orderBy("channel_status_key")
)
