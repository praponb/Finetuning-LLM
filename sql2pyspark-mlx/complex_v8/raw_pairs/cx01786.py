from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter((F.col("approved_amount") >= 2569.2) & (F.col("claim_type") == "delay"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

detail_rows = (
    valid_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount") >= 0.0), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

tagged = (
    detail_rows
    .filter(F.col("adjuster_id") <= 7)
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "claim_id",
        "claim_reported_at",
        "claim_claim_date",
        F.date_trunc("hour", F.col("claim_reported_at")).alias("claim_reported_at_hour"),
    )
)

rollup_base = (
    tagged
    .filter(F.year(F.col("claim_claim_date")) == 2025)
    .groupBy(
        F.date_format("claim_claim_date", "yyyy-MM").alias("claim_claim_date_ym"),
        "claim_id",
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
    )
    .agg(
        F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"),
        F.round(100 * F.count(F.when(F.col("claim_type").isNotNull(), 1)) / F.count("*"), 1).alias("pct_claim_type_is_not_null"),
        F.sum("approved_amount").alias("sum_approved_amount_check"),
    )
    .filter(F.col("sum_approved_amount_check") > 1273.7)
    .drop("sum_approved_amount_check")
)

result = (
    rollup_base
    .select(
        "claim_claim_date_ym",
        "claim_date_ym",
        "median_claimed_amount",
        "pct_claim_type_is_not_null",
    )
    .orderBy(F.col("pct_claim_type_is_not_null").desc(), "median_claimed_amount")
)
