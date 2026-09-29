from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 6]) | (F.col("last_restock") <= "2025-02-01"))
)

summary = (
    valid_inventory
    .groupBy("product_id")
    .agg(F.max("last_restock").alias("last_last_restock"))
)

result = summary
