from pyspark.sql import functions as F

filtered_order_items = (
    df_order_items
    .filter((F.col("product_id").isin([4, 24]) & F.col("product_id").isin([27, 39])) | F.col("quantity").between(3, 5))
)

quantity_bucket_summary = (
    filtered_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 5, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)

result = (
    quantity_bucket_summary
    .filter(F.col("revenue") > 51)
    .select("quantity_bucket", "avg_quantity", "revenue")
)
