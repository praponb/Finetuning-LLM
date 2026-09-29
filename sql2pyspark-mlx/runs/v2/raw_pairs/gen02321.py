from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(F.col("order_date").between("2023-06-01", "2025-10-01") & F.col("customer_id").isin([9, 14, 35]))
)

channel_summary = (
    recent_orders
    .groupBy("channel")
    .agg(
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.count("discount").alias("discount_count"),
    )
)

result = (
    channel_summary
    .select("channel", "stddev_shipping_fee", "discount_count")
    .orderBy(F.col("discount_count").desc(), "channel")
    .limit(1)
)
