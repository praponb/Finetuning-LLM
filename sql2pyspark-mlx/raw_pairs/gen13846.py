from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("shipping_fee") > 11)
    .groupBy("store_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(F.col("s.store_name"), F.col("t.row_count"))
)
