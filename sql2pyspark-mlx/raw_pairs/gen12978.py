from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") <= 0.31)
    .withColumn("channel_prefix", F.substring("channel", 1, 2))
)
