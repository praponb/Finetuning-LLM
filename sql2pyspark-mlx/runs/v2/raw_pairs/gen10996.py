from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(F.col("customer_id").isin([5, 9, 14]))
    .groupBy("store_id")
    .agg(
        F.count("*").alias("cnt"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("ss.cnt"),
        F.col("ss.total_shipping_fee"),
    )
    .orderBy(F.col("region").desc())
)
