from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter((F.col("paid_at") < "2023-10-24") & (F.col("method") != "bank_transfer") & ~F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
)

paid_month_summary = (
    filtered_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.max("paid_at").alias("last_paid_at"),
    )
)

result = (
    paid_month_summary
    .select("paid_month", "failed_count", "last_paid_at")
    .orderBy(F.col("paid_month").desc())
)
