from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("quantity") > 6)
    .groupBy("order_id")
    .agg(F.min("unit_price").alias("min_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(F.col("o.status"), F.col("os.min_unit_price"))
    .orderBy("min_unit_price")
)
