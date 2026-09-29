from pyspark.sql import functions as F

src = df_payments.filter(F.col("method") == "promptpay")

status_summary = (
    src
    .groupBy("status")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.min("paid_at").alias("min_paid_at"),
    )
)

result = (
    status_summary
    .select("status", "total_amount", "min_paid_at")
    .orderBy("total_amount")
)
