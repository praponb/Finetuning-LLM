from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter(F.col("claimed_amount") < 43724.9)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        F.when(F.col("claimed_amount") >= 41878.5, "HIGH").when(F.col("claimed_amount") >= 13898.1, "MEDIUM").otherwise("LOW").alias("claimed_amount_tier"),
    )
)

status_stats = (
    filtered_claims
    .groupBy("status")
    .agg(F.percentile_approx("approved_amount", 0.5).alias("median_approved_amount"))
)

outliers = (
    filtered_claims.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.approved_amount") >= F.col("g.median_approved_amount"))
    .select(
        "s.*",
        "g.median_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.median_approved_amount"), 2).alias("approved_amount_minus_median"),
    )
)

with_details = (
    outliers.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.claimed_amount_tier",
        "s.median_approved_amount",
        "s.approved_amount_minus_median",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("po.segment").alias("holder_segment"),
        F.col("po.risk_score").alias("holder_risk_score"),
    )
)

with_details_policy = (
    with_details.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status").isNotNull()), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount_tier",
        "s.median_approved_amount",
        "s.approved_amount_minus_median",
        "s.policy_premium",
        "s.policy_status",
        "s.policy_end_date",
        "s.holder_risk_score",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date_2"),
    )
)

result = (
    with_details_policy
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount_tier",
        "policy_premium",
        "policy_status",
        "policy_end_date",
        "policy_end_date_2",
        F.round(F.col("median_approved_amount") / F.nullif(F.col("holder_risk_score"), F.lit(0)), 3).alias("median_approved_amount_per_holder_risk_score"),
    )
    .orderBy(F.col("policy_id").desc(), "policy_end_date_2", "id")
    .limit(5)
)
