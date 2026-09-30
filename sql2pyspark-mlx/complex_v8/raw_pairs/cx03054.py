from pyspark.sql import functions as F

valid = (
    df_claims
    .filter(F.col("claim_type") == "delay")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

detail_rows_claims = (
    valid.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

result = (
    detail_rows_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_id",
        "claim_reported_at",
        "claim_claimed_amount",
        "claim_policy_id",
        F.date_trunc("hour", F.col("claim_reported_at")).alias("claim_reported_at_hour"),
    )
    .orderBy(F.col("claim_id").desc(), F.col("policy_id").desc_nulls_first())
)
