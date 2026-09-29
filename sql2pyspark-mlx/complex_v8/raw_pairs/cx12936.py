from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.abs(F.col("approved_amount") - 10355.0).alias("approved_amount_distance"),
    )
)

exploded_detail_claims = (
    filtered_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.status_len",
        "s.approved_amount_filled",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.status").alias("claim_status"),
    )
)

result = (
    exploded_detail_claims
    .select(
        "id",
        "adjuster_id",
        "status_len",
        "claim_id",
        "claim_claim_date",
        "claim_policy_id",
        "claim_reported_at",
    )
    .orderBy("id", F.col("claim_claim_date").desc_nulls_first())
)
