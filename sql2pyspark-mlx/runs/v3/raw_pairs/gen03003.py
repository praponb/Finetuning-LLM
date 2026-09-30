from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("status").isin(["COMPLETED", "RETURNED"]))
    .groupBy("store_id")
    .agg(F.min("order_date").alias("min_order_date"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(F.col("s.region"), F.col("ss.min_order_date"))
    .orderBy("region", "min_order_date")
)
