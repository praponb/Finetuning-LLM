from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("order_date") <= "2024-07-01")
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("t.pending_count"),
        F.col("t.avg_shipping_fee"),
    )
)
