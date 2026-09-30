from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.countDistinct("revenue").alias("unique_stores"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.total_revenue") >= 16000)
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("ss.total_revenue"),
        F.col("ss.unique_stores"),
    )
    .orderBy(F.col("store_name").desc(), F.col("sqft").desc())
)
