from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3]))
    .groupBy("store_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("StoreStats"), F.col("stores.id") == F.col("StoreStats.store_id"), "left")
    .filter(F.col("StoreStats.stddev_quantity") >= 76)
    .select(
        F.col("stores.sqft"),
        F.col("stores.region"),
        F.col("StoreStats.stddev_quantity"),
    )
)
