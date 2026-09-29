from pyspark.sql import functions as F

valid = (
    df_claims
    .filter(F.col("status") == "REJECTED")
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
        F.date_trunc("month", F.col("reported_at")).alias("reported_at_month"),
    )
)

joined = (
    valid.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.reported_at",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.status").alias("policy_status"),
    )
)

with_children = (
    joined.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") < 14993.0), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.policy_holder_id",
        "s.policy_status",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

result = (
    with_children
    .groupBy("claim_claim_type")
    .pivot("claim_type", ["flood", "delay", "collision"])
    .agg(F.sum("claim_approved_amount"))
    .orderBy("claim_claim_type")
)
