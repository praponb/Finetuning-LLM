from pyspark.sql import functions as F

base = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.concat_ws(": ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

with_children_claims = (
    base.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.status_claim_type_label",
        "s.claim_date_label",
        "s.status_or_unknown",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

joined_claim = (
    with_children_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status_claim_type_label",
        "s.claim_date_label",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_claimed_amount",
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

derived = (
    joined_claim
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "claim_id",
        "claim_adjuster_id",
        "claim_policy_id",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.coalesce(F.col("claim_claimed_amount"), F.lit(0.0)).alias("claim_claimed_amount_filled"),
        F.coalesce(F.col("claim_claimed_amount"), F.lit(0.0)).alias("claim_claimed_amount_filled_2"),
    )
)

with_children_claims_2 = (
    derived.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_policy_id",
        "s.claim_claimed_amount_filled",
        "s.claim_claimed_amount_filled_2",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.adjuster_id").alias("claim_adjuster_id_2"),
    )
)

combined_details = (
    with_children_claims_2.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.claim_adjuster_id_2"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_policy_id",
        "s.claim_claimed_amount_filled",
        "s.claim_claimed_amount_filled_2",
        "s.claim_id_2",
        "s.claim_claim_type",
        "s.claim_reported_at",
        "s.claim_adjuster_id_2",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    combined_details
    .filter(F.col("claim_claim_type").isin("medical", "delay"))
    .select(
        "id",
        "policy_id",
        "claim_id",
        "claim_adjuster_id",
        "claim_policy_id",
        "claim_claimed_amount_filled_2",
        "claim_id_2",
        "claim_claim_type",
        "adjuster_team",
    )
    .orderBy(F.col("claim_claim_type").desc_nulls_first())
)
