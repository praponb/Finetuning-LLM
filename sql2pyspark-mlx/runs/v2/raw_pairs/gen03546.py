from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("shipping_fee").between(16, 18) & F.col("channel").isin(["web", "partner"]) & (F.col("store_id") == 10))
)

orders_stats = (
    base
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
        F.min("discount").alias("min_discount"),
    )
)

result = (
    orders_stats
    .filter(F.col("shipped_count") > 4)
    .select("order_quarter", "shipped_count", "min_discount")
)
