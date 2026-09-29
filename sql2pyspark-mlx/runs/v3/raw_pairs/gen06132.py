from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("discount") <= 0.3)

orders_stats = (
    filtered_orders
    .groupBy("store_id", "channel")
    .agg(F.min("order_date").alias("min_order_date"))
)

result = orders_stats.orderBy(F.col("store_id").desc())
