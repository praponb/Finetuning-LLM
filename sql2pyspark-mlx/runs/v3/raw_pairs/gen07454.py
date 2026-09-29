from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") == 203) & (F.col("store_id") == 6) & (F.col("last_restock") >= "2024-12-14"))
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.min("quantity").alias("min_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy(F.col("quantity_count").desc())
)
