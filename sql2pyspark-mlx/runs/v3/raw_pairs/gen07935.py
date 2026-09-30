from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(F.col("s.city"), F.col("ss.max_amount"), F.col("ss.stddev_discount"))
    .orderBy("stddev_discount")
)
