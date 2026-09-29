from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("ss.avg_amount"))
    .orderBy(F.col("avg_amount").desc(), "id")
    .limit(10)
)
