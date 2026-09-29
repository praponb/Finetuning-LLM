from pyspark.sql import functions as F

filtered_payments = df_payments.filter(F.col("method") == "cash")

paid_month_summary = (
    filtered_payments
    .groupBy(F.month("paid_at").alias("paid_month"), "status")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("paid_at").alias("last_paid_at"),
    )
)

result = paid_month_summary.select("paid_month", "status", "amount_range", "last_paid_at")
