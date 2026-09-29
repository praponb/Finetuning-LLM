from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
        F.avg("shipping_fee").alias("avg_shipping_fee"),
    )
    .filter(F.col("avg_shipping_fee") > 33)
    .drop("avg_shipping_fee")
    .orderBy("latest_order_date", "channel")
    .limit(3)
)
