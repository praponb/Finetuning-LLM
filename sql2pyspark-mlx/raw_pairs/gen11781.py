from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") <= "2024-01-07") | F.col("product_id").isin([2, 3, 10]))
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.min("last_restock").alias("min_last_restock"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
    .filter(F.col("unique_stores") >= 5)
    .drop("unique_stores")
    .orderBy(F.col("store_id").desc())
)
