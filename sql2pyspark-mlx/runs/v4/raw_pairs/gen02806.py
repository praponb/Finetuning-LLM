from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("store_id").isin([28, 38]))
    .groupBy("store_id")
    .agg(F.sort_array(F.collect_set("channel")).alias("channel_list"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "inner")
    .select(F.col("s.city"), F.col("obs.channel_list"))
    .orderBy("city")
)
