from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([11, 29]) & F.col("order_id").isin([34, 38, 40]))
    .groupBy(
        F.when(F.col("quantity") >= 6, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
        "order_id",
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") >= 3.8)
)
