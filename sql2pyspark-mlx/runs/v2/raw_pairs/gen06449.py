from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-07-01", "2023-08-01") & F.col("store_id").isin([3, 4, 5, 6]))
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.min("last_restock").alias("min_last_restock"),
    )
)
