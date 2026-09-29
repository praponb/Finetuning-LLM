from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("S.stddev_revenue"),
    )
    .orderBy(F.col("region").desc())
)
