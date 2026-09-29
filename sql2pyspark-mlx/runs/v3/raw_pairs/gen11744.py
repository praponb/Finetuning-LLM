from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("t.stddev_revenue"),
    )
    .orderBy(F.col("store_name").desc(), F.col("stddev_revenue").desc())
)
