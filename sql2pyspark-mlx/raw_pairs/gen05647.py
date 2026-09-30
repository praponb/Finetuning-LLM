from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") >= "2024-04-01")
    .groupBy("channel")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
)
