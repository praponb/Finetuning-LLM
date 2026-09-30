from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.max("discount").alias("max_discount"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.region"),
        F.col("ss.max_discount"),
        F.col("ss.min_amount"),
    )
)
