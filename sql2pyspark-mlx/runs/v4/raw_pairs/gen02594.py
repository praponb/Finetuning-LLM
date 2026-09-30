from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
    )
)

result = (
    df_stores.alias("stores")
    .join(orders_by_store.alias("orders_by_store"), F.col("stores.id") == F.col("orders_by_store.store_id"), "left")
    .filter(F.col("orders_by_store.stddev_amount") >= 25)
    .select(
        F.col("stores.region"),
        F.col("stores.store_name"),
        F.col("orders_by_store.stddev_amount"),
        F.col("orders_by_store.shipped_count"),
    )
    .orderBy(F.col("region").desc())
)
