from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(F.col("amount") >= 790)
    .groupBy("store_id")
    .agg(F.max("order_date").alias("last_order_date"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("s.opened_date"), F.col("ss.last_order_date"))
)
