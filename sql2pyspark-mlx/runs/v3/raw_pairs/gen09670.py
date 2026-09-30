from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2025-10-19")
    .agg(F.max("last_restock").alias("last_last_restock"))
    .orderBy("last_last_restock")
)
