from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([3, 19, 29]))
    .groupBy(
        "product_id",
        "order_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("min_quantity") < 6.4)
)
