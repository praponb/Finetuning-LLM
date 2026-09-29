from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") <= 280) & (F.col("unit_price") >= 100))
)

order_items_stats = (
    recent_order_items
    .groupBy("order_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)

result = order_items_stats.select("order_id", "unit_price_range")
