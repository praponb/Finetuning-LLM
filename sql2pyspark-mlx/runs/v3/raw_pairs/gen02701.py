from pyspark.sql import functions as F

store_totals = (
    df_orders
    .groupBy("store_id")
    .agg(F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.web_count") > 6)
    .select(F.col("s.id"), F.col("s.region"), F.col("t.web_count"))
)
