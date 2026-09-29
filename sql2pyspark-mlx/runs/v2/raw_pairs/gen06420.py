from pyspark.sql import functions as F

recent_payments = df_payments.filter(F.col("paid_at") < "2025-10-30")

summary = (
    recent_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"), "method")
    .agg(
        F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0)).alias("promptpay_count"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    summary
    .select("paid_month", "method", "promptpay_count")
    .orderBy(F.col("promptpay_count").desc(), "method")
)
