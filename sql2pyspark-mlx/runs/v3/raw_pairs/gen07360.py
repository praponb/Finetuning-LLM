from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(F.count("shipping_fee").alias("shipping_fee_count"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("s.id"), F.col("s.city"), F.col("ss.shipping_fee_count"))
    .orderBy(F.col("city").desc())
)
