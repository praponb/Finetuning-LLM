from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.avg_amount") >= 22)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.region"),
        F.col("ss.latest_order_date"),
        F.col("ss.avg_amount"),
    )
    .orderBy("city", F.col("id").desc())
)
