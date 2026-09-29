from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter((F.col("status") == "IN_REVIEW") & (F.col("claim_type") == "collision"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

claim_side = (
    df_claims
    .filter(F.col("status") == "APPROVED")
    .select(
        "id",
        F.col("reported_at").alias("event_time"),
        F.col("claim_type").alias("category"),
    )
)

claim_side_2 = (
    df_claims
    .filter(F.col("claim_type").isin("fire", "collision", "flood"))
    .select(
        F.col("policy_id").alias("id"),
        F.col("reported_at").alias("event_time"),
        F.col("claim_type").alias("category"),
    )
)

result = (
    claim_side
    .union(claim_side_2)
    .distinct()
)
