from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(F.countDistinct("discount").alias("unique_channels"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "left")
    .select(F.col("s.opened_date"), F.col("s.city"), F.col("obs.unique_channels"))
)
