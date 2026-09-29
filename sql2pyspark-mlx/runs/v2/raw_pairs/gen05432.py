from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([8, 9]))
    .groupBy("store_id", "product_id")
    .agg(F.max("last_restock").alias("last_last_restock"))
)
