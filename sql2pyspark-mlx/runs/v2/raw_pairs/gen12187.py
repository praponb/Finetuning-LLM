from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.col("amount").between(920, 930) & (F.col("paid_at") < "2023-09-14"))
)

status_summary = (
    recent_payments
    .groupBy("status")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = status_summary.select("status", "amount_range")
