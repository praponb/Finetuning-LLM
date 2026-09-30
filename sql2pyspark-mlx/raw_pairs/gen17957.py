from pyspark.sql import functions as F

recent_order_items = df_order_items.filter((F.col("order_id") == 19) & (F.col("product_id") == 14))

quantity_bucket_summary = (
    recent_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.count("*").alias("total_count"),
    )
)

result = (
    quantity_bucket_summary
    .select("quantity_bucket", "unique_orders", "total_count")
    .orderBy("total_count")
)
