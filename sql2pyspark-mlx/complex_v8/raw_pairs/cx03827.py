from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .filter((F.col("claim_type") == "fire") & (F.col("approved_amount") < 23207.7))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
        F.add_months(F.col("claim_date"), 6).alias("claim_date_plus_6m"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_ref"),
    )
)

with_parent = (
    scoped_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
    )
)

enriched = (
    with_parent.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.status_prefix",
        "s.claim_type_ref",
        "s.adjuster_name",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    enriched
    .cube("adjuster_team", "policy_holder_id", "adjuster_id")
    .agg(
        F.countDistinct("policy_holder_id").alias("distinct_policy_holder_ids"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
    )
    .orderBy(
        F.col("adjuster_team").asc_nulls_last(),
        F.col("policy_holder_id").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
