from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.col("product_id").isin([2, 4, 7, 9]))
    .groupBy("store_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("store_stats"), F.col("stores.id") == F.col("store_stats.store_id"), "inner")
    .filter(F.col("store_stats.avg_quantity") > 170)
    .select(
        F.col("stores.id"),
        F.col("stores.region"),
        F.col("store_stats.avg_quantity"),
    )
)
