from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 10) | (F.col("store_id") == 5))
    .groupBy("store_id")
    .agg(F.max("last_restock").alias("last_last_restock"))
    .orderBy(F.col("last_last_restock").desc())
)
