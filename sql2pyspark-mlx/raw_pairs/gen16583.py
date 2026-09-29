from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter((F.col("quantity") < 87) & (F.col("last_restock") < "2025-01-05") & F.col("store_id").isin([1, 4, 5]))
)

inventory_stats = (
    filtered_inventory
    .groupBy("store_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    inventory_stats
    .filter(F.col("max_quantity") < 130)
    .orderBy("store_id")
)
