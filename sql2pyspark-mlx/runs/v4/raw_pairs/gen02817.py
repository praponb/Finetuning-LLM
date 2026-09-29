from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("order_date").between("2025-04-29", "2025-10-01") & F.col("shipping_fee").between(10, 36))
)

orders_stats = (
    base
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
    )
)

result = (
    orders_stats
    .select("order_month", "shipped_count", "stddev_shipping_fee")
    .orderBy("shipped_count")
)
