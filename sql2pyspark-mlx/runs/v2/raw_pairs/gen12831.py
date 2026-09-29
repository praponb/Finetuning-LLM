from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("channel").isin(["store", "web", "partner"]))
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.amount_range") > 75)
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("t.total_count"),
        F.col("t.amount_range"),
    )
    .orderBy(F.col("total_count").desc(), "id")
    .limit(1)
)
