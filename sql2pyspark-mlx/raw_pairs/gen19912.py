from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.quantity_range") >= 50)
    .select(F.col("o.id"), F.col("o.channel"), F.col("os.quantity_range"))
    .orderBy("quantity_range")
)
