from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 4, 5]) & (F.col("last_restock") > "2025-08-14"))
    .withColumn("quantity_rounded", F.round("quantity", 2))
    .orderBy("id")
    .limit(10)
)
