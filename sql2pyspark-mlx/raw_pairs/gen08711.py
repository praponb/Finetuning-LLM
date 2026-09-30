from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("store_id").isin([10, 31, 34]))
    .groupBy("store_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("store_stats"), F.col("stores.id") == F.col("store_stats.store_id"), "inner")
    .filter(F.col("store_stats.pct_cancelled") >= 68)
    .select(
        F.col("stores.id"),
        F.col("stores.sqft"),
        F.col("store_stats.pct_cancelled"),
    )
    .orderBy(F.col("sqft").desc(), "id")
    .limit(50)
)
