from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("channel").isin(["partner", "mobile"]))
    .groupBy("store_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.max_amount") > 950)
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("ss.max_amount"),
    )
    .orderBy("id")
)
