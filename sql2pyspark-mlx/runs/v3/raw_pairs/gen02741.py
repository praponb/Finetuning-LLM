from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") >= "2024-09-01") | F.col("last_restock").between("2023-11-01", "2023-12-01"))
    .groupBy(
        F.when(F.col("quantity") >= 237, "high").when(F.col("quantity") >= 65, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("min_quantity") >= 130)
)
