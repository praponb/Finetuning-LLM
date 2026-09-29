from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter(F.col("reported_at") < F.lit("2025-02-18 15:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

claim_side = valid_claims.select(F.col("claim_type_or_unknown").alias("name"), "claimed_amount")

holder_side = (
    df_policyholders
    .select(
        F.col("segment").alias("name"),
        F.col("risk_score").alias("claimed_amount"),
    )
)

result = (
    claim_side
    .unionByName(holder_side)
    .distinct()
    .orderBy("name")
)
