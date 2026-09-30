from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2025-07-16") | (F.col("paid_at") < "2024-08-01"))
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.max("paid_at").alias("latest_paid_at"),
    )
    .orderBy(F.col("refunded_count").desc(), "order_id")
    .limit(1)
)
