from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(F.count("*").alias("inventory_count"))
    .orderBy(F.col("inventory_count").desc())
)
