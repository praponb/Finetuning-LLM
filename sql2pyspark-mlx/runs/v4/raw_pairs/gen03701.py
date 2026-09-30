from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.cancelled_count") > 50)
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("ss.cancelled_count"),
        F.col("ss.avg_amount"),
    )
    .orderBy("cancelled_count")
)
