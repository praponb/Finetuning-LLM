from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("inventory_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("ss.min_quantity"),
        F.col("ss.inventory_count"),
    )
)
