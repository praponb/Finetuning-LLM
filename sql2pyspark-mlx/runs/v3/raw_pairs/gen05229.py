from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(F.sort_array(F.collect_set("channel")).alias("channel_list"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("O"), F.col("s.id") == F.col("O.store_id"), "left")
    .select(F.col("s.region"), F.col("O.channel_list"))
    .orderBy("region")
)
