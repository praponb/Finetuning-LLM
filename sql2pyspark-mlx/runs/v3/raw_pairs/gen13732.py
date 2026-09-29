from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("quantity").between(82, 127))

inventory_stats = (
    recent_inventory
    .groupBy("store_id")
    .agg(F.max("last_restock").alias("latest_last_restock"))
)

result = inventory_stats.select("store_id", "latest_last_restock")
