from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(F.col("discount") > 0.26)
    .groupBy("store_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("s.id"), F.col("s.region"), F.col("ss.pct_cancelled"))
    .orderBy("region")
)
