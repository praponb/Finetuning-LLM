from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.count("revenue").alias("revenue_count"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.revenue_count") >= 9)
    .select(F.col("s.id"), F.col("s.sqft"), F.col("t.revenue_count"))
)
