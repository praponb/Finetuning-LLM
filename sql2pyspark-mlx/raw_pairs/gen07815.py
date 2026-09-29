from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id").isin([2, 3, 5]) & F.col("store_id").isin([2, 6])) | F.col("store_id").isin([2, 4, 5, 6]))
    .groupBy("store_id", "product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("product_id").desc())
)
