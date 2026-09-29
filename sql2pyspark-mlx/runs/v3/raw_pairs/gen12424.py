from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([4, 5, 8]) & (F.col("shipping_fee") < 39) & (F.col("shipping_fee") <= 39))
    .groupBy("channel", "status", "customer_id")
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.max("order_date").alias("last_order_date"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
    .filter(F.col("max_shipping_fee") > 30)
)
