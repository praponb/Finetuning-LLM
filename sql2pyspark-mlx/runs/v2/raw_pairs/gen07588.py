from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)

result = (
    df_stores.alias("stores")
    .join(orders_by_store.alias("t"), F.col("stores.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.region"),
        F.col("stores.city"),
        F.col("t.last_order_date"),
        F.col("t.channel_values"),
    )
)
