from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 5, 6]))
    .groupBy("store_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("t.avg_revenue"),
    )
)
