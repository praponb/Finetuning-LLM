from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("unit_price") > 310)
    .groupBy("order_id")
    .agg(F.min("unit_price").alias("min_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.min_unit_price") >= 170)
    .select(F.col("o.id"), F.col("o.store_id"), F.col("os.min_unit_price"))
    .orderBy(F.col("store_id").desc())
)
