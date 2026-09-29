from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg(
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("s.opened_date"), F.col("ss.shipping_fee_range"), F.col("ss.cnt"))
)
