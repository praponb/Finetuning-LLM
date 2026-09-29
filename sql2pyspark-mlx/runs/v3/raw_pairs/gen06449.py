from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("channel") == "partner")
    .groupBy("store_id")
    .agg(F.count("discount").alias("discount_count"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.discount_count") > 6)
    .select(F.col("s.store_name"), F.col("t.discount_count"))
    .orderBy(F.col("store_name").desc())
)
