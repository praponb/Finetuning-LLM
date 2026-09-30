from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(F.min("last_restock").alias("first_last_restock"))
    .orderBy("first_last_restock")
)
