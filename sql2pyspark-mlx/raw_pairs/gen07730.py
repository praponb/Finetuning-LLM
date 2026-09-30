from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "left")
    .filter(F.col("obs.total_count") > 4)
    .select(
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("obs.stddev_amount"),
        F.col("obs.total_count"),
    )
)
