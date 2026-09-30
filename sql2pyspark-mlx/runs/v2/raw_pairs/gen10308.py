from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 8, 10]))
    .groupBy("store_id")
    .agg(F.count("revenue").alias("revenue_count"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.revenue_count") > 8)
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("t.revenue_count"))
)
