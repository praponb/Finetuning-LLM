from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.count("revenue").alias("revenue_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("t.unique_stores"),
        F.col("t.revenue_count"),
    )
    .orderBy(F.col("unique_stores").desc())
)
