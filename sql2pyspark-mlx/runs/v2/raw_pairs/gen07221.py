from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.count("discount").alias("discount_count"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.discount_count") >= 6)
    .select(F.col("s.city"), F.col("t.discount_count"), F.col("t.stddev_amount"))
    .orderBy(F.col("stddev_amount").desc())
)
