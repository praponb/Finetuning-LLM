from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        (F.max("discount") - F.min("discount")).alias("discount_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.discount_range") > 77)
    .select(
        F.col("s.city"),
        F.col("s.region"),
        F.col("ss.latest_order_date"),
        F.col("ss.discount_range"),
    )
    .orderBy(F.col("discount_range").desc(), "city")
)
