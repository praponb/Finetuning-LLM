from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg(F.countDistinct("discount").alias("unique_channels"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.unique_channels") >= 8)
    .select(F.col("s.id"), F.col("s.sqft"), F.col("ss.unique_channels"))
)
