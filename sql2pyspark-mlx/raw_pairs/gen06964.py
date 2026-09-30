from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("quantity").isNotNull() | (F.col("quantity") < 5))

order_items_stats = (
    recent_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        F.when(F.col("unit_price") >= 440, "high").when(F.col("unit_price") >= 50, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    order_items_stats
    .select("quantity_bucket", "unit_price_bucket", "max_quantity")
    .orderBy("quantity_bucket")
)
