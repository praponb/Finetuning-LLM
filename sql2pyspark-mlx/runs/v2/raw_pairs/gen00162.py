from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("shipping_fee") >= 17)
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "store", 1).otherwise(0))) / F.count("*")), 1).alias("pct_store"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "inner")
    .select(
        F.col("s.region"),
        F.col("s.city"),
        F.col("obs.stddev_discount"),
        F.col("obs.pct_store"),
    )
    .orderBy(F.col("stddev_discount").desc(), "city")
)
