from pyspark.sql import functions as F

store_totals = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.stddev_shipping_fee") >= 21)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("t.stddev_shipping_fee"),
        F.col("t.avg_amount"),
    )
    .orderBy("city")
)
