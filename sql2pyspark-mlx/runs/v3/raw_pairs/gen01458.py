from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("channel").isin(["web", "partner"]))
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("channel") == "store", F.col("amount")).otherwise(0)).alias("store_amount"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.amount_count") >= 6)
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("ss.store_amount"),
        F.col("ss.amount_count"),
    )
)
