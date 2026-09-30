from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2024-03-01")
    .groupBy(
        F.when(F.col("quantity") >= 267, "high").when(F.col("quantity") >= 32, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("unique_products") >= 2)
)
