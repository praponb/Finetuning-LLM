from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2025-01-31")
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.avg("quantity").alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
