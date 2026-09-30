from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.avg_revenue") > 54)
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("ss.total_units"),
        F.col("ss.avg_revenue"),
    )
    .orderBy("total_units", F.col("sqft").desc(), "id")
    .limit(10)
)
