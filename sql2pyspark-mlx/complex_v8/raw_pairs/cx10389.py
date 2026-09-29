from pyspark.sql import functions as F

open_monthly = (
    df_claims
    .filter(F.col("status") == "OPEN")
    .groupBy(F.date_trunc("month", "claim_date").alias("claim_date_month"))
    .agg(
        F.count("*").alias("open_count"),
        F.sum("claimed_amount").alias("open_total_claimed_amount"),
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
    open_monthly.alias("a")
    .join(approved_monthly.alias("b"), F.col("a.claim_date_month") == F.col("b.claim_date_month"), "full")
    .select(
        F.coalesce(F.col("a.claim_date_month"), F.col("b.claim_date_month")).alias("claim_date_month"),
        F.coalesce(F.col("a.open_count"), F.lit(0)).alias("open_count"),
        F.col("a.open_total_claimed_amount"),
        F.coalesce(F.col("b.approved_count"), F.lit(0)).alias("approved_count"),
        F.coalesce(F.col("b.approved_total_claimed_amount"), F.lit(0)).alias("approved_total_claimed_amount"),
        (F.coalesce(F.col("a.open_count"), F.lit(0)) - F.coalesce(F.col("b.approved_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("claim_date_month")
)
