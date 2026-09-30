from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.unit_price_count") >= 7)
    .select(F.col("o.status"), F.col("o.channel"), F.col("os.unit_price_count"))
    .orderBy(F.col("status").desc(), F.col("unit_price_count").desc())
)
