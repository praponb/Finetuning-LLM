from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") == 4)
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 5, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
    .filter(F.col("unique_products") > 8)
    .orderBy(F.col("avg_unit_price").desc())
)
