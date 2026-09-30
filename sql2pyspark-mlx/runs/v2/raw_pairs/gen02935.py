from pyspark.sql import functions as F

valid_order_items = df_order_items.filter(F.col("quantity") <= 6)

order_items_stats = (
    valid_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 190, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.count("*").alias("order_item_count"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = order_items_stats.filter(F.col("total_unit_price") >= 1500)
