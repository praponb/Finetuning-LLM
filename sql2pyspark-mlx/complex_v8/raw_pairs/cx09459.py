from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter((F.col("status").isNotNull()) | (F.col("adjuster_id") >= 6))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

claim_side = (
    df_claims
    .filter(F.col("claimed_amount") <= 40638.4)
    .select("policy_id", F.col("claim_type").alias("label"), "reported_at")
)

claim_side_2 = (
    df_claims
    .filter(F.col("status") == "PAID")
    .select(
        F.col("id").alias("policy_id"),
        F.col("claim_type").alias("label"),
        "reported_at",
    )
)

claim_rows = (
    df_claims
    .select(
        F.col("adjuster_id").alias("policy_id"),
        F.col("status").alias("label"),
        "reported_at",
    )
)

result = (
    claim_side
    .unionByName(claim_side_2)
    .unionByName(claim_rows)
)
