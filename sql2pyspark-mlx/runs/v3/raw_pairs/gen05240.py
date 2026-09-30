from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("amount") > 640)
    .groupBy("store_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.pct_mobile") > 48)
    .select(F.col("s.id"), F.col("s.region"), F.col("t.pct_mobile"))
)
