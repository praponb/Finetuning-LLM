from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("store_id").isin([1, 3, 5]))

product_id_summary = (
    recent_inventory
    .groupBy("product_id")
    .agg(F.min("last_restock").alias("min_last_restock"))
)

result = product_id_summary
