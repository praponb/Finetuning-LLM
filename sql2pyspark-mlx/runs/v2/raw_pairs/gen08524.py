from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.total_count") > 6)
    .select(F.col("s.city"), F.col("s.sqft"), F.col("t.total_count"))
)
