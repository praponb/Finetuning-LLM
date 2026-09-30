from pyspark.sql import functions as F

base = df_orders.filter(F.col("store_id") == 25)

orders_stats = (
    base
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"), "channel")
    .agg(
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.max("order_date").alias("last_order_date"),
    )
)

result = (
    orders_stats
    .select("order_month", "channel", "stddev_shipping_fee", "last_order_date")
    .orderBy("stddev_shipping_fee", "order_month", "channel")
    .limit(5)
)
