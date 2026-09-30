from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter((F.col("discount") <= 0.28) & (F.col("status") == "PENDING") & (F.col("amount") >= 1260))
)

store_id_summary = (
    recent_orders
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("shipping_fee")).otherwise(0)).alias("shipped_shipping_fee"),
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
    )
)

result = store_id_summary.orderBy(F.col("completed_count").desc())
