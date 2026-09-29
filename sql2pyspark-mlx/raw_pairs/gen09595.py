from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("status").isin(["CANCELLED", "SHIPPED"]))
    .groupBy("store_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("S"), F.col("s.id") == F.col("S.store_id"), "left")
    .filter(F.col("S.pct_shipped") >= 35)
    .select(
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("S.pct_shipped"),
        F.col("S.pending_count"),
    )
    .orderBy(F.col("pending_count").desc())
)
