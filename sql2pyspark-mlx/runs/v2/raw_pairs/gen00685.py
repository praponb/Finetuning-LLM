from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") < "2024-05-01") & (F.year("last_restock") >= 2024))
    .groupBy("store_id", F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") >= 150)
    .orderBy(F.col("min_quantity").desc())
)
