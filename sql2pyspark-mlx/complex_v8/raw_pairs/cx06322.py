from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("claim_date") >= F.lit("2024-04-11").cast("date")) & (F.col("approved_amount").between(2272.7, 32511.8)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        (F.col("reported_at") + F.expr("INTERVAL 30 MINUTES")).alias("reported_at_plus_30m"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
    )
)

with_parent_policy = (
    filtered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.approved_amount_per_claimed_amount",
        F.col("p.status").alias("policy_status"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
    )
)

filtered_2 = (
    with_parent_policy.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount") < 44991.5), "left_semi")
)

with_children = (
    filtered_2.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.approved_amount_per_claimed_amount",
        "s.policy_status",
        "s.policy_end_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        "c.claim_type",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

scored = (
    with_children
    .select(
        "*",
        F.round(F.col("claim_claimed_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("claim_claimed_amount_per_claimed_amount"),
    )
)

enriched_claim = (
    scored.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .filter(F.col("c.policy_id").isNotNull())
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.policy_status",
        "s.policy_end_date",
        "s.claim_id",
        "s.claim_policy_id",
        "s.claim_type",
        "s.claim_claimed_amount_per_claimed_amount",
        F.col("c.policy_id").alias("claim_policy_id_2"),
    )
)

result = (
    enriched_claim
    .filter(F.col("claimed_amount").between(17759.3, 33975.8))
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "status",
        "policy_status",
        "claim_id",
        "claim_policy_id",
        "claim_type",
        "claim_claimed_amount_per_claimed_amount",
        "claim_policy_id_2",
    )
    .orderBy(F.col("status").desc())
)
