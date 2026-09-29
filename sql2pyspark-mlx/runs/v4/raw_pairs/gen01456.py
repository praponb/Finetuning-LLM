from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.count("quantity").alias("quantity_count"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("min_unit_price") >= 290)
    .orderBy(F.col("product_id").desc())
)
