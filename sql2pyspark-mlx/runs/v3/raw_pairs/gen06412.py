from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 9]) | (F.col("last_restock") > "2024-03-01"))
    .groupBy(
        F.when(F.col("quantity") >= 246, "high").when(F.col("quantity") >= 47, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.count("quantity").alias("quantity_count"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("quantity_count") >= 4)
    .orderBy(F.col("unique_products").desc())
)
