from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(F.col("o.id"), F.col("o.order_date"), F.col("os.quantity_count"))
)
