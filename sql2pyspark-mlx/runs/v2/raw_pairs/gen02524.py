from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "left")
    .filter(F.col("obs.avg_amount") >= 1430)
    .select(F.col("s.opened_date"), F.col("s.region"), F.col("obs.avg_amount"))
)
