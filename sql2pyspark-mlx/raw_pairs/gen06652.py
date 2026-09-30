from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 6]) & F.col("quantity").between(134, 229))
    .agg(F.min("last_restock").alias("first_last_restock"))
    .orderBy("first_last_restock")
)
