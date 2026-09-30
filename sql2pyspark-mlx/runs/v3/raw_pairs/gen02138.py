from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([4, 8, 11]))
    .groupBy("store_id")
    .agg(F.min("last_restock").alias("first_last_restock"))
    .orderBy(F.col("first_last_restock").desc())
)
