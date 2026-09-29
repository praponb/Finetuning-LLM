from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("store_id").isin([18, 33]))
    .groupBy("store_id")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.region"), F.col("ss.avg_amount"))
)
