from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("total_count"),
        F.min("unit_price").alias("min_unit_price"),
    )
)
