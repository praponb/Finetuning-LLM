from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.count("shipping_fee").alias("shipping_fee_count"),
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.max("order_date").alias("last_order_date"),
    )
    .filter(F.col("stddev_shipping_fee") > 60)
    .orderBy("shipping_fee_count", "stddev_shipping_fee", "channel")
    .limit(3)
)
