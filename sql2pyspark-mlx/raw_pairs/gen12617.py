from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", "product_id")
    .agg(F.max("last_restock").alias("latest_last_restock"))
    .orderBy("latest_last_restock")
)
