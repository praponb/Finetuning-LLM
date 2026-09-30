from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "store"]))
    .groupBy("channel")
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.avg("discount").alias("avg_discount"),
    )
    .filter(F.col("avg_discount") < 0.14)
)
