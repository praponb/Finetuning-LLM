from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("unit_price") <= 120)

order_items_stats = (
    filtered_order_items
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = order_items_stats.select("product_id", "quantity_range")
