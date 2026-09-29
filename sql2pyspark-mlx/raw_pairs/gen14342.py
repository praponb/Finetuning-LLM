from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 10]))
    .groupBy("store_id")
    .agg(F.min("last_restock").alias("min_last_restock"))
    .orderBy(F.col("store_id").desc())
)
