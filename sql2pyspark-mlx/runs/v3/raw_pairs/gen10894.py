from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("order_date") < "2025-08-05")
    .groupBy("store_id")
    .agg(F.min("order_date").alias("first_order_date"))
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("store_stats"), F.col("stores.id") == F.col("store_stats.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.store_name"),
        F.col("stores.opened_date"),
        F.col("store_stats.first_order_date"),
    )
    .orderBy("opened_date", "id")
    .limit(1)
)
