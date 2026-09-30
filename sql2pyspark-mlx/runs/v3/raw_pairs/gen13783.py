from pyspark.sql import functions as F

valid_payments = df_payments.filter(F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]))

paid_month_summary = (
    valid_payments
    .groupBy(F.month("paid_at").alias("paid_month"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
    )
)

result = paid_month_summary.orderBy("paid_month")
