from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter((F.col("order_id") == 19) & (F.hour("paid_at").between(6, 9) | (F.col("status") == "REFUNDED")))
)

payments_stats = (
    valid_payments
    .groupBy(F.hour("paid_at").alias("paid_hour"), "status")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.min("paid_at").alias("min_paid_at"),
    )
)

result = payments_stats.filter(F.col("total_amount") < 8800)
