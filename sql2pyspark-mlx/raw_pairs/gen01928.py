from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 60, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("max_quantity") >= 7.0)
    .select("product_id", "unit_price_bucket", "max_quantity", "avg_quantity")
    .orderBy(F.col("max_quantity").desc(), F.col("product_id").desc())
)
