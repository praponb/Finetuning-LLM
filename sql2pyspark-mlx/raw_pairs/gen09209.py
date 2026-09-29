from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 5]))
    .agg(F.max("last_restock").alias("last_last_restock"))
    .orderBy("last_last_restock")
)
