from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("units") <= 19)
    .groupBy("store_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.total_count") >= 9)
    .select(F.col("s.city"), F.col("s.opened_date"), F.col("ss.total_count"))
    .orderBy("total_count")
)
