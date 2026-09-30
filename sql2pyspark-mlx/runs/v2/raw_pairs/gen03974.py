from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter(F.col("paid_at") >= "2025-04-16")
    .withColumn("status_title", F.initcap(F.trim("status")))
)

method_summary = (
    filtered_payments
    .groupBy("method")
    .agg(
        F.min("amount").alias("min_amount"),
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
    )
)

result = (
    method_summary
    .filter(F.col("min_amount") >= 810)
    .select("method", "min_amount", "pct_refunded")
    .orderBy("min_amount")
)
