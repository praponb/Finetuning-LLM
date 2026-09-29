from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("status") == "CANCELLED")
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("discount")).otherwise(0)).alias("mobile_discount"),
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("t.mobile_discount"),
        F.col("t.shipping_fee_range"),
    )
)
