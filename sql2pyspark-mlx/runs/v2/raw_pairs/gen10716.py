from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.min("order_date").alias("min_order_date"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("S"), F.col("s.id") == F.col("S.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("S.min_order_date"),
        F.col("S.shipped_count"),
    )
    .orderBy(F.col("shipped_count").desc())
)
