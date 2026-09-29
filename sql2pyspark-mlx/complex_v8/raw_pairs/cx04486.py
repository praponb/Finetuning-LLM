from pyspark.sql import functions as F

base = (
    df_claims
    .filter((F.col("claimed_amount") > 19347.2) & (F.col("policy_id") >= 8))
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

combined_details_policy = (
    base.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        F.col("p.start_date").alias("policy_start_date"),
    )
)

line_level = (
    combined_details_policy.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id").isNotNull()), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    line_level
    .groupBy("claim_policy_id")
    .pivot("claim_type", ["medical", "delay", "collision"])
    .agg(F.count("approved_amount").alias("count_v"), F.max("approved_amount").alias("max_v"))
    .orderBy("claim_policy_id")
)
