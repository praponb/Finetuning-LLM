from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2]))
    .groupBy("store_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.min("revenue").alias("min_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .filter(F.col("S.min_revenue") > 2300)
    .select(
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("S.avg_revenue"),
        F.col("S.min_revenue"),
    )
)
