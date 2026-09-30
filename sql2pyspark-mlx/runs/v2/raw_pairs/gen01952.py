from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(~F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]) & (F.col("amount") >= 1110))
)

amount_bucket_summary = (
    recent_payments
    .groupBy(
        F.when(F.col("amount") >= 1590, "high").when(F.col("amount") >= 610, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"))
)

result = amount_bucket_summary.orderBy(F.col("amount_bucket").desc())
