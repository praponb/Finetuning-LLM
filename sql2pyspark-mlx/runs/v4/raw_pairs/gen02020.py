from pyspark.sql import functions as F

recent_orders = df_orders.filter(F.col("store_id").isin([7, 18]))

orders_stats = (
    recent_orders
    .groupBy("status")
    .agg(F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"))
)

result = (
    orders_stats
    .filter(F.col("shipped_count") >= 7)
    .select("status", "shipped_count")
    .orderBy("status", F.col("shipped_count").desc())
)
