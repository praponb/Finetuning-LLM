from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "inner")
    .filter(F.col("obs.pct_mobile") >= 38)
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("obs.pct_mobile"),
        F.col("obs.partner_count"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
