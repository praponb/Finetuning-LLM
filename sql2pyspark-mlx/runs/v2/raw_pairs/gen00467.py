from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 6, 7]))
    .groupBy("store_id")
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.city"), F.col("ss.revenue_range"))
    .orderBy("city", "id")
    .limit(3)
)
