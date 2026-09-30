from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("inventory_count"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("store_stats"), F.col("stores.id") == F.col("store_stats.store_id"), "left")
    .filter(F.col("store_stats.inventory_count") > 7)
    .select(
        F.col("stores.id"),
        F.col("stores.city"),
        F.col("store_stats.total_count"),
        F.col("store_stats.inventory_count"),
    )
    .orderBy(F.col("id").desc())
)
