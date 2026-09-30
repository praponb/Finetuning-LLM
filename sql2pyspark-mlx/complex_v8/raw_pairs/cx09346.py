from pyspark.sql import functions as F

covered = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.status",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

with_details_policy = (
    covered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.adjuster_id",
        "s.product",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

with_children = (
    with_details_policy.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id") >= 17), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.adjuster_id",
        "s.policy_premium",
        "s.policy_end_date",
        "s.policy_holder_id",
        "s.adjuster_level",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.policy_id").alias("claim_policy_id"),
        "c.claim_type",
    )
)

result = (
    with_children
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "policy_premium",
        "policy_holder_id",
        "adjuster_level",
        "claim_id",
        "claim_approved_amount",
        "claim_type",
        F.round(F.col("policy_premium") / F.nullif(F.col("adjuster_level"), F.lit(0)), 3).alias("policy_premium_per_adjuster_level"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("policy_premium_per_adjuster_level").desc(),
        "id",
        "claim_id",
    )
    .limit(20)
)
