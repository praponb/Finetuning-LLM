from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
    )
    .filter(F.col("failed_count") >= 38)
    .select("paid_month", "pct_success", "failed_count")
    .orderBy("failed_count", "paid_month")
    .limit(1)
)
