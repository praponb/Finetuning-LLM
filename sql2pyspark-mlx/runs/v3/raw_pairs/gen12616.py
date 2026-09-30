from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("order_summary"), F.col("orders.id") == F.col("order_summary.order_id"), "inner")
    .filter(F.col("order_summary.total_count") >= 8)
    .select(
        F.col("orders.customer_id"),
        F.col("orders.store_id"),
        F.col("order_summary.total_count"),
    )
    .orderBy("total_count")
)
