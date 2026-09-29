from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(F.min("last_restock").alias("min_last_restock"))
    .orderBy("min_last_restock")
)
