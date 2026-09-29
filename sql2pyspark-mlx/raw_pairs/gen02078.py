from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "REFUNDED") | (F.to_date("paid_at") < "2024-07-31"))
    .groupBy("order_id")
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
