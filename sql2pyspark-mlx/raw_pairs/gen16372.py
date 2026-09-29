from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .groupBy("store_id")
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.count("*").alias("num_orders"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("store_stats"), F.col("stores.id") == F.col("store_stats.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.opened_date"),
        F.col("store_stats.discount_range"),
        F.col("store_stats.num_orders"),
    )
)
