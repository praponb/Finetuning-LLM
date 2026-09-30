from pyspark.sql import functions as F

base_claims = (
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
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.abs(F.col("claimed_amount") - 19337.6).alias("claimed_amount_distance"),
        F.least(F.col("claimed_amount"), F.col("approved_amount")).alias("least_claimed_amount_approved_amount"),
    )
)

annotated = base_claims.select("*", F.upper(F.trim(F.col("status_ref"))).alias("status_ref_clean"))

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.count("*").alias("claim_count"),
    )
)

profiled = (
    annotated.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.first_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

joined_policy = (
    profiled.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.status_ref",
        "s.claimed_amount_distance",
        "s.least_claimed_amount_approved_amount",
        "s.status_ref_clean",
        "s.first_claim_reported_at",
        "s.claim_count",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.premium").alias("policy_premium"),
        "a.adjuster_name",
    )
)

result = (
    joined_policy
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "reported_at",
        "status_ref",
        "claimed_amount_distance",
        "least_claimed_amount_approved_amount",
        "first_claim_reported_at",
        "claim_count",
        F.round((F.unix_timestamp(F.col("reported_at")) - F.unix_timestamp(F.col("first_claim_reported_at"))) / 3600, 1).alias("hours_first_claim_reported_at_to_reported_at"),
        F.round(F.col("claim_count") / F.nullif(F.col("policy_premium"), F.lit(0)), 3).alias("claim_count_per_policy_premium"),
    )
)
