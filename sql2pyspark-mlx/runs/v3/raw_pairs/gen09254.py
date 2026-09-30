from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg(F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(F.col("s.opened_date"), F.col("ss.web_count"))
)
