from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter(F.col("method").isNotNull() & (F.col("paid_at") < "2024-08-01"))
    .select("*", F.trim("status").alias("clean_status"))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("amount") >= 1700, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
    )
)
