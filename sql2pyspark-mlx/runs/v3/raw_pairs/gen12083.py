from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 8]))
    .groupBy("product_id")
    .agg(F.max("last_restock").alias("latest_last_restock"))
    .orderBy(F.col("product_id").desc())
    .limit(5)
)
