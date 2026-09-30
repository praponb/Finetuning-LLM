from pyspark.sql import functions as F

rejected_monthly = (
    df_claims
    .filter(F.col("status") == "REJECTED")
    .groupBy(F.date_trunc("month", "claim_date").alias("claim_date_month"))
    .agg(
        F.count("*").alias("rejected_count"),
        F.round(F.avg("claimed_amount"), 2).alias("rejected_avg_claimed_amount"),
    )
)

paid_monthly = (
    df_claims
    .filter(F.col("status") == "PAID")
    .groupBy(F.date_trunc("month", "claim_date").alias("claim_date_month"))
    .agg(
        F.count("*").alias("paid_count"),
        F.round(F.avg("claimed_amount"), 2).alias("paid_avg_claimed_amount"),
    )
)

result = (
    rejected_monthly.alias("a")
    .join(paid_monthly.alias("b"), F.col("a.claim_date_month") == F.col("b.claim_date_month"), "full")
    .select(
        F.coalesce(F.col("a.claim_date_month"), F.col("b.claim_date_month")).alias("claim_date_month"),
        F.coalesce(F.col("a.rejected_count"), F.lit(0)).alias("rejected_count"),
        F.coalesce(F.col("a.rejected_avg_claimed_amount"), F.lit(0)).alias("rejected_avg_claimed_amount"),
        F.coalesce(F.col("b.paid_count"), F.lit(0)).alias("paid_count"),
        F.coalesce(F.col("b.paid_avg_claimed_amount"), F.lit(0)).alias("paid_avg_claimed_amount"),
        (F.coalesce(F.col("a.rejected_count"), F.lit(0)) - F.coalesce(F.col("b.paid_count"), F.lit(0))).alias("rejected_count_minus_paid_count"),
    )
    .orderBy("claim_date_month")
)
