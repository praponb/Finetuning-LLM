from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("product_id") == 34)

order_items_stats = (
    recent_order_items
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)

result = order_items_stats.orderBy(F.col("product_id").desc())
