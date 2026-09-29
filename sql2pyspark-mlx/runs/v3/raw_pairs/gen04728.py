from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2025-03-01")
    .groupBy("product_id")
    .agg(F.min("last_restock").alias("min_last_restock"))
)
