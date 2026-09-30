from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 6, 8]))
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.stddev_revenue") > 38)
    .select(F.col("s.region"), F.col("t.stddev_revenue"), F.col("t.revenue_range"))
    .orderBy(F.col("stddev_revenue").desc())
)
