from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(~F.col("channel").isin(["mobile", "web", "partner"]))
    .groupBy("store_id")
    .agg(F.count("*").alias("num_orders"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("ss.num_orders"),
    )
    .orderBy("city")
)
