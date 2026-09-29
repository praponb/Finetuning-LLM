from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "inner")
    .filter(F.col("obs.avg_shipping_fee") >= 64)
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("obs.avg_shipping_fee"))
)
