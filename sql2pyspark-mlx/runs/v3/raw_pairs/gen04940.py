from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.city"), F.col("ss.cnt"))
    .orderBy("city")
)
