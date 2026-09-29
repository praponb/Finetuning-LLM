from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.avg_unit_price") >= 370)
    .select(F.col("o.status"), F.col("t.avg_unit_price"))
)
