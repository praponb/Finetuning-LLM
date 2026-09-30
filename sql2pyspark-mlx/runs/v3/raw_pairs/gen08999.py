from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .filter(F.col("units") > 26)
    .groupBy("store_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .filter(F.col("S.max_revenue") >= 2500)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("S.max_revenue"),
        F.col("S.stddev_revenue"),
    )
)
