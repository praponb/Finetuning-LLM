from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(F.max("order_date").alias("latest_order_date"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("ss.latest_order_date"),
    )
    .orderBy(F.col("latest_order_date").desc())
)
