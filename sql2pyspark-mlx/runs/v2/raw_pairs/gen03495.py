from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.unit_price_count") > 9)
    .select(F.col("o.channel"), F.col("o.status"), F.col("os.unit_price_count"))
)
