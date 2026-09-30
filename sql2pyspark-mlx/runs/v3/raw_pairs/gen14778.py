from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 6]))
    .groupBy("store_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("total_count"),
    )
)
