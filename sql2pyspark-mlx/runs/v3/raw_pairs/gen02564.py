from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("shipping_fee") >= 38)
    .groupBy("store_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.max("order_date").alias("last_order_date"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("StoreStats"), F.col("stores.id") == F.col("StoreStats.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.opened_date"),
        F.col("stores.sqft"),
        F.col("StoreStats.channel_values"),
        F.col("StoreStats.last_order_date"),
    )
)
