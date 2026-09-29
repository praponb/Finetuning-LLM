from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("channel") != "store")
    .groupBy("store_id")
    .agg(F.avg("discount").alias("avg_discount"))
)

result = (
    df_stores.alias("stores")
    .join(orders_by_store.alias("orders_by_store"), F.col("stores.id") == F.col("orders_by_store.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.city"),
        F.col("stores.region"),
        F.col("orders_by_store.avg_discount"),
    )
)
