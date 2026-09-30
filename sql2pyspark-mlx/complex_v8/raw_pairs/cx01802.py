from pyspark.sql import functions as F

paid_monthly = (
    df_claims
    .filter(F.col("status") == "PAID")
    .groupBy(F.date_trunc("month", "claim_date").alias("claim_date_month"))
    .agg(
        F.count("*").alias("paid_count"),
        F.sum("claimed_amount").alias("paid_total_claimed_amount"),
    )
)

rejected_monthly = (
    df_claims
    .filter(F.col("status") == "REJECTED")
    .groupBy(F.date_trunc("month", "claim_date").alias("claim_date_month"))
    .agg(
        F.count("*").alias("rejected_count"),
        F.sum("claimed_amount").alias("rejected_total_claimed_amount"),
    )
)

result = (
    paid_monthly.alias("a")
    .join(rejected_monthly.alias("b"), F.col("a.claim_date_month") == F.col("b.claim_date_month"), "full")
    .select(
        F.coalesce(F.col("a.claim_date_month"), F.col("b.claim_date_month")).alias("claim_date_month"),
        F.coalesce(F.col("a.paid_count"), F.lit(0)).alias("paid_count"),
        F.col("a.paid_total_claimed_amount"),
        F.coalesce(F.col("b.rejected_count"), F.lit(0)).alias("rejected_count"),
        F.col("b.rejected_total_claimed_amount"),
        (F.coalesce(F.col("a.paid_count"), F.lit(0)) - F.coalesce(F.col("b.rejected_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("claim_date_month")
)
