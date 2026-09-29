from pyspark.sql import functions as F

base = (
    df_claims
    .filter(F.col("claim_date") < F.lit("2025-05-30").cast("date"))
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
    )
)

joined_policy = (
    base.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status") == "ACTIVE"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        F.col("p.status").alias("policy_status"),
    )
)

joined = (
    joined_policy.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.policy_status",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    joined
    .cube("policy_status", "adjuster_id", "policy_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.stddev("policy_premium"), 2).alias("stddev_policy_premium"),
    )
    .orderBy(
        F.col("policy_status").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
