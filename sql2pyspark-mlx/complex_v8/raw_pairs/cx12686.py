from pyspark.sql import functions as F

rejected_monthly = (
    df_claims
    .filter(F.col("status") == "REJECTED")
    .groupBy(F.date_trunc("month", "claim_date").alias("claim_date_month"))
    .agg(
        F.count("*").alias("rejected_count"),
        F.sum("claimed_amount").alias("rejected_total_claimed_amount"),
    )
)

approved_monthly = (
    df_claims
    .filter(F.col("status") == "APPROVED")
    .groupBy(F.date_trunc("month", "claim_date").alias("claim_date_month"))
    .agg(
        F.count("*").alias("approved_count"),
        F.sum("claimed_amount").alias("approved_total_claimed_amount"),
    )
)

result = (
    rejected_monthly.alias("a")
    .join(approved_monthly.alias("b"), F.col("a.claim_date_month") == F.col("b.claim_date_month"), "full")
    .select(
        F.coalesce(F.col("a.claim_date_month"), F.col("b.claim_date_month")).alias("claim_date_month"),
        F.coalesce(F.col("a.rejected_count"), F.lit(0)).alias("rejected_count"),
        F.coalesce(F.col("a.rejected_total_claimed_amount"), F.lit(0)).alias("rejected_total_claimed_amount"),
        F.coalesce(F.col("b.approved_count"), F.lit(0)).alias("approved_count"),
        F.col("b.approved_total_claimed_amount"),
        (F.coalesce(F.col("a.rejected_count"), F.lit(0)) - F.coalesce(F.col("b.approved_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("claim_date_month")
)
