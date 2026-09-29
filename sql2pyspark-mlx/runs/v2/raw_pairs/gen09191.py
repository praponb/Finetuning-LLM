from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(~F.col("status").isin(["COMPLETED", "RETURNED"]))
    .groupBy("store_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
        (F.max("discount") - F.min("discount")).alias("discount_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("ss.pct_web"),
        F.col("ss.discount_range"),
    )
    .orderBy(F.col("discount_range").desc(), "city", "id")
    .limit(5)
)
