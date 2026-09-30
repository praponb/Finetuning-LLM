from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.countDistinct("units").alias("unique_products"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("ss.avg_revenue"),
        F.col("ss.unique_products"),
    )
    .orderBy(F.col("unique_products").desc())
)
