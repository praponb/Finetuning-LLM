from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.quantity_count") >= 9)
    .select(F.col("o.id"), F.col("o.customer_id"), F.col("os.quantity_count"))
    .orderBy(F.col("id").desc(), F.col("quantity_count").desc())
)
