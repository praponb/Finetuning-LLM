from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("units") == 20)
    .groupBy("store_id")
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("t"), F.col("stores.id") == F.col("t.store_id"), "left")
    .select(F.col("stores.id"), F.col("stores.region"), F.col("t.revenue_range"))
    .orderBy(F.col("id").desc())
)
