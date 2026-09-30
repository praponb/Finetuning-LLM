from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("customer_id") == 17)
    .groupBy("store_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.stddev_amount") > 78)
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("t.avg_amount"),
        F.col("t.stddev_amount"),
    )
)
