from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("status").isin(["CANCELLED", "COMPLETED", "SHIPPED"]))
    .groupBy("store_id")
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.sum("discount").alias("total_discount"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("obs.avg_discount"),
        F.col("obs.total_discount"),
    )
    .orderBy("city", "id")
)
