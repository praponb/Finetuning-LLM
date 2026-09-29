from pyspark.sql import functions as F

valid = (
    df_claims
    .filter((F.col("claimed_amount") <= 39666.5) & (F.to_date(F.col("reported_at")) < F.lit("2025-03-08").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
        F.when(F.col("approved_amount") >= 23487.5, "large").when(F.col("approved_amount") >= 9389.3, "medium").otherwise("small").alias("approved_amount_tier"),
    )
)

combined_details_policy = (
    valid.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.claim_type_prefix",
        "s.approved_amount_tier",
        F.col("p.status").alias("policy_status"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("po.joined_on").alias("holder_joined_on"),
        F.col("po.segment").alias("holder_segment"),
    )
)

line_level_claims = (
    combined_details_policy.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.claim_type_prefix",
        "s.approved_amount_tier",
        "s.policy_status",
        "s.policy_start_date",
        "s.holder_joined_on",
        "s.holder_segment",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

joined = (
    line_level_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.id") == F.col("s.claim_id")) & (F.col("c.status").isin("PAID", "REJECTED", "APPROVED")), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.claim_type_prefix",
        "s.approved_amount_tier",
        "s.policy_status",
        "s.policy_start_date",
        "s.holder_joined_on",
        "s.holder_segment",
        "s.claim_id",
        "s.claim_type",
        "s.claim_reported_at",
        "s.claim_claimed_amount",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        "c.claim_date",
        F.col("c.status").alias("claim_status"),
    )
)

with_activity = (
    joined
    .crossJoin(joined.agg(F.min("claim_claimed_amount").alias("overall_min_claim_claimed_amount")))
    .filter(F.col("claim_claimed_amount") > F.col("overall_min_claim_claimed_amount"))
    .drop("overall_min_claim_claimed_amount")
)

result = (
    with_activity
    .filter(F.col("holder_joined_on").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "claim_type_prefix",
        "approved_amount_tier",
        "policy_status",
        "policy_start_date",
        "holder_joined_on",
        "claim_id",
        "claim_type",
        "claim_reported_at",
        "claim_claimed_amount",
        "claim_adjuster_id",
        "claim_date",
        "claim_status",
        (F.col("claim_reported_at") + F.expr("INTERVAL 1 DAYS")).alias("claim_reported_at_plus_1d"),
    )
)
