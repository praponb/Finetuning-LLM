from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("s.store_name"), F.col("ss.cnt"), F.col("ss.stddev_revenue"))
)
