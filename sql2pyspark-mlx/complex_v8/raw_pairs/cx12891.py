from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter((F.col("claim_type").isNotNull()) & (F.col("reported_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

claim_side = (
    cleaned_claims
    .filter(F.hour(F.col("reported_at")).between(6, 11))
    .select(F.col("adjuster_id").alias("ref_id"), "claim_type")
)

policy_rows = df_policies.select(F.col("id").alias("ref_id"), F.col("status").alias("claim_type"))

result = (
    claim_side
    .subtract(policy_rows)
    .orderBy("ref_id", "claim_type")
)
