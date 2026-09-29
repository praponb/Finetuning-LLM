from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("ss.total_count"),
        F.col("ss.stddev_revenue"),
    )
    .orderBy("id")
)
