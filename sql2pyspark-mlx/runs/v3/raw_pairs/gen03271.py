from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("order_id").isNull() & ((F.col("paid_at") >= "2025-01-01") | F.col("amount").between(720, 1240)))
)

summary = (
    valid_payments
    .groupBy("status")
    .agg(
        F.count("amount").alias("amount_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
    )
)

result = (
    summary
    .filter(F.col("pct_failed") < 58)
    .select("status", "pct_failed")
    .orderBy("status", F.col("pct_failed").desc())
    .limit(1)
)
