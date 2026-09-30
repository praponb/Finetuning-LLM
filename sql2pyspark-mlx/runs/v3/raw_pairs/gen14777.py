from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("product_id").isNotNull())

order_items_stats = (
    filtered_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)

result = order_items_stats
