from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("store_stats"), F.col("stores.id") == F.col("store_stats.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.opened_date"),
        F.col("store_stats.partner_count"),
    )
    .orderBy("id")
)
