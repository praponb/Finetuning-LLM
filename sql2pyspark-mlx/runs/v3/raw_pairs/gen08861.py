from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id", "channel")
    .agg(F.avg("discount").alias("avg_discount"))
    .orderBy("channel", F.col("avg_discount").desc())
)
