from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(F.max("order_date").alias("latest_order_date"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(F.col("s.city"), F.col("s.sqft"), F.col("t.latest_order_date"))
)
