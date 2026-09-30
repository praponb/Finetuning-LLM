from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull() & (F.col("channel") == "partner"))
    .groupBy("status")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
    )
)
