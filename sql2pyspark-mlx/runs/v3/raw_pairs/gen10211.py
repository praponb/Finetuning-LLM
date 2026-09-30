from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)

result = (
    df_stores.alias("stores")
    .join(orders_by_store.alias("orders_by_store"), F.col("stores.id") == F.col("orders_by_store.store_id"), "inner")
    .select(F.col("stores.region"), F.col("orders_by_store.channel_values"))
    .orderBy(F.col("channel_values").desc(), "region")
)
