from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("ss.avg_revenue"),
        F.col("ss.units_range"),
    )
    .orderBy(F.col("id").desc())
)
