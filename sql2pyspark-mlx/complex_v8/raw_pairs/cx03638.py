from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter(~F.col("claim_type").isin("delay", "fire"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

exploded_detail_claims = (
    filtered.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claimed_amount_minus_approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

with_activity = (
    exploded_detail_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status").isNotNull()) & (F.col("c.adjuster_id") >= 2), "left_anti")
    .filter(F.col("status") != "REJECTED")
)

enriched_claim = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.claim_adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.claimed_amount_minus_approved_amount",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_claim_date",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("a.team").alias("adjuster_team"),
    )
)

line_level = (
    enriched_claim.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.claimed_amount_minus_approved_amount",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.adjuster_team",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.approved_amount").alias("claim_approved_amount_2"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "c.claim_date",
    )
)

result = (
    line_level
    .select(
        "id",
        "claim_id",
        "claim_approved_amount_2",
        "claimed_amount_minus_approved_amount",
        "claim_claimed_amount",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["claim_approved_amount_2", "claimed_amount_minus_approved_amount", "claim_claimed_amount"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
