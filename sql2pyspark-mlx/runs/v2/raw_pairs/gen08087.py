from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(F.col("discount") < 0.13)
    .groupBy("store_id")
    .agg(F.min("order_date").alias("first_order_date"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("S"), F.col("s.id") == F.col("S.store_id"), "left")
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("S.first_order_date"))
)
