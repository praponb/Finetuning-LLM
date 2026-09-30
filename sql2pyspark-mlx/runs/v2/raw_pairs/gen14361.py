from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "inner")
    .filter(F.col("obs.mobile_count") >= 6)
    .select(
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("obs.mobile_count"),
        F.col("obs.pct_shipped"),
    )
    .orderBy(F.col("pct_shipped").desc())
)
