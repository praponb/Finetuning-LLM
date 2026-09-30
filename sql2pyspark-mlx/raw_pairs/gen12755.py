from pyspark.sql import functions as F

recent_orders = df_orders.filter(F.col("store_id").isNotNull() & F.col("store_id").isin([11, 34]))

orders_stats = (
    recent_orders
    .groupBy("status")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    orders_stats
    .select("status", "avg_amount")
    .orderBy("status")
)
