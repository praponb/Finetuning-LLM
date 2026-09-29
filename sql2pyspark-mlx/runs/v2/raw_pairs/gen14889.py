from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "status", "customer_id")
    .agg(
        F.avg("discount").alias("avg_discount"),
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
    )
    .filter(F.col("pending_count") < 69)
    .orderBy("pending_count")
)
