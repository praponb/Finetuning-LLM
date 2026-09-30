from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("total_quantity") < 30)
    .select("quantity_bucket", "product_id", "total_quantity", "avg_quantity")
    .orderBy(F.col("product_id").desc())
)
