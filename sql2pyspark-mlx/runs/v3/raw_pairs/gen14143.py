from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.count("units").alias("units_count"),
        F.sum("revenue").alias("total_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("ss.units_count"),
        F.col("ss.total_revenue"),
    )
    .orderBy(F.col("units_count").desc(), F.col("id").desc())
)
