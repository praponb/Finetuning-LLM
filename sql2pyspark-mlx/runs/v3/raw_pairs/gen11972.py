from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("amount") >= 1310)
    .groupBy("store_id")
    .agg(F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "inner")
    .select(F.col("s.city"), F.col("s.opened_date"), F.col("obs.web_count"))
    .orderBy("web_count", F.col("city").desc())
)
