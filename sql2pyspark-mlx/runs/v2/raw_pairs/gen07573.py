from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2, 4, 5]))
    .groupBy("store_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.max("units").alias("max_units"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("store_stats"), F.col("stores.id") == F.col("store_stats.store_id"), "left")
    .filter(F.col("store_stats.total_revenue") >= 22000)
    .select(
        F.col("stores.id"),
        F.col("stores.city"),
        F.col("store_stats.total_revenue"),
        F.col("store_stats.max_units"),
    )
)
