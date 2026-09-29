from pyspark.sql import functions as F

paid_monthly = (
    df_claims
    .filter(F.col("status") == "PAID")
    .groupBy(F.date_trunc("month", "claim_date").alias("claim_date_month"))
    .agg(
        F.count("*").alias("paid_count"),
        F.round(F.avg("claimed_amount"), 2).alias("paid_avg_claimed_amount"),
    )
)

approved_monthly = (
    df_claims
    .filter(F.col("status") == "APPROVED")
    .groupBy(F.date_trunc("month", "claim_date").alias("claim_date_month"))
    .agg(
        F.count("*").alias("approved_count"),
        F.round(F.avg("claimed_amount"), 2).alias("approved_avg_claimed_amount"),
    )
)

result = (
    paid_monthly.alias("a")
    .join(approved_monthly.alias("b"), F.col("a.claim_date_month") == F.col("b.claim_date_month"), "full")
    .select(
        F.coalesce(F.col("a.claim_date_month"), F.col("b.claim_date_month")).alias("claim_date_month"),
        F.coalesce(F.col("a.paid_count"), F.lit(0)).alias("paid_count"),
        F.col("a.paid_avg_claimed_amount"),
        F.coalesce(F.col("b.approved_count"), F.lit(0)).alias("approved_count"),
        F.col("b.approved_avg_claimed_amount"),
        (F.coalesce(F.col("a.paid_count"), F.lit(0)) - F.coalesce(F.col("b.approved_count"), F.lit(0))).alias("paid_count_minus_approved_count"),
    )
    .orderBy("claim_date_month")
)
