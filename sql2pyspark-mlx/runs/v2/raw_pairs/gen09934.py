from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .filter(F.col("revenue") >= 1400)
    .groupBy("store_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("S"), F.col("s.id") == F.col("S.store_id"), "left")
    .filter(F.col("S.avg_units") > 42)
    .select(F.col("s.city"), F.col("S.avg_units"), F.col("S.stddev_revenue"))
)
