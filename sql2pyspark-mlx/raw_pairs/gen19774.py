from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "left")
    .filter(F.col("obs.stddev_amount") > 25)
    .select(F.col("s.city"), F.col("s.region"), F.col("obs.stddev_amount"))
    .orderBy("region")
)
