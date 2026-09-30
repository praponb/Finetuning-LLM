from pyspark.sql import functions as F

valid_payments = df_payments.filter(F.col("status").isin(["FAILED", "REFUNDED"]))

method_summary = (
    valid_payments
    .groupBy("method")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = method_summary.filter(F.col("pct_failed") < 24)
