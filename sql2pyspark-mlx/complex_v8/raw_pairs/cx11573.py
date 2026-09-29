from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter(F.col("claim_type") != "theft")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        F.least(F.col("claimed_amount"), F.col("approved_amount")).alias("least_claimed_amount_approved_amount"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.substring(F.trim(F.col("claim_type")), 1, 3).alias("claim_type_prefix"),
    )
)

summary = (
    cleaned_claims
    .groupBy(
        "policy_id",
        F.date_format("reported_at", "yyyy-MM").alias("reported_at_ym"),
    )
    .agg(
        F.percentile_approx("approved_amount", 0.5).alias("median_approved_amount"),
        F.min("reported_at").alias("first_reported_at"),
        F.sum("least_claimed_amount_approved_amount").alias("sum_least_claimed_amount_approved_amount_check"),
    )
    .filter(F.col("sum_least_claimed_amount_approved_amount_check") > 7500.0)
    .drop("sum_least_claimed_amount_approved_amount_check")
)

screened = (
    summary.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
    .filter(F.col("median_approved_amount") <= 32500.0)
)

labeled = (
    screened
    .filter(F.col("first_reported_at") < F.lit("2025-02-24 11:00:00").cast("timestamp"))
    .select(
        "policy_id",
        "reported_at_ym",
        "median_approved_amount",
        "first_reported_at",
        (F.col("first_reported_at") + F.expr("INTERVAL 7 DAYS")).alias("first_reported_at_plus_7d"),
    )
)

without_matches = (
    labeled.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount") < 30115.9) & (F.col("c.claim_type") != "delay"), "left_anti")
)

result = (
    without_matches
    .select(
        "policy_id",
        "reported_at_ym",
        "median_approved_amount",
        "first_reported_at",
        "first_reported_at_plus_7d",
    )
    .orderBy(F.col("policy_id").desc_nulls_first(), "reported_at_ym")
    .limit(10)
)
