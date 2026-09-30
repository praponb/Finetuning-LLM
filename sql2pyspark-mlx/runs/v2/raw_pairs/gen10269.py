from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("channel") == "web")
    .groupBy("store_id")
    .agg(F.count("discount").alias("discount_count"))
)

result = (
    df_stores.alias("stores")
    .join(orders_by_store.alias("orders_by_store"), F.col("stores.id") == F.col("orders_by_store.store_id"), "left")
    .filter(F.col("orders_by_store.discount_count") > 5)
    .select(
        F.col("stores.id"),
        F.col("stores.region"),
        F.col("orders_by_store.discount_count"),
    )
    .orderBy("region", "id")
)
