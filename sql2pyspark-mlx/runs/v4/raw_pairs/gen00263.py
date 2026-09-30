from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter((F.col("quantity") < 4) & F.col("product_id").isNull())

quantity_bucket_summary = (
    filtered_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    quantity_bucket_summary
    .orderBy(F.col("total_unit_price").desc(), "quantity_bucket", "product_id")
    .limit(50)
)
