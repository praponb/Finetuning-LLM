from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("first_last_restock").desc())
)
