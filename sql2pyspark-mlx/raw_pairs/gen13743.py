from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5, 6]) & (F.year("last_restock") == 2024))
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(F.count("*").alias("num_inventory"))
    .orderBy(F.col("last_restock_quarter").desc())
)
