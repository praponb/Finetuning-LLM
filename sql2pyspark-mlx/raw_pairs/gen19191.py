from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .filter(F.col("units").isNull())
    .groupBy("store_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.min("revenue").alias("min_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("ss.avg_revenue"),
        F.col("ss.min_revenue"),
    )
    .orderBy(F.col("min_revenue").desc())
)
