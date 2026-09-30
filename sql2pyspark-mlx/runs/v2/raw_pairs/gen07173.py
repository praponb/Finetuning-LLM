from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .groupBy("store_id")
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.total_shipping_fee") >= 210)
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("ss.total_shipping_fee"),
    )
    .orderBy("store_name", "id")
    .limit(5)
)
