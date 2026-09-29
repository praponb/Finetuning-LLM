from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("unit_price") >= 280)

order_items_stats = (
    recent_order_items
    .groupBy("order_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = order_items_stats.select("order_id", "total_quantity")
