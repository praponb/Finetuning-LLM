from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        (F.max("discount") - F.min("discount")).alias("discount_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("ss.avg_discount"),
        F.col("ss.discount_range"),
    )
)
