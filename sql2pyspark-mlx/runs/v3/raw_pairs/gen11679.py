from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", F.month("order_date").alias("order_month"))
    .agg(
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
    .orderBy(F.col("avg_shipping_fee").desc())
)
