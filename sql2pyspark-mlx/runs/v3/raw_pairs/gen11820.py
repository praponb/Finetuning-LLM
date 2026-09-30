from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("channel") == "web", F.col("amount")).otherwise(0)).alias("web_amount"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("ss.web_amount"),
        F.col("ss.min_shipping_fee"),
    )
    .orderBy("web_amount", "id")
    .limit(10)
)
