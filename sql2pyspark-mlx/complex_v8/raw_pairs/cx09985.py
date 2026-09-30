from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter(F.col("claim_date").isNotNull())
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
        F.concat(F.col("claim_type"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_ref"),
    )
)

claim_side = (
    valid_claims
    .filter((F.col("approved_amount") >= 4554.8) & (F.col("claim_type_ref").isNotNull()))
    .select("adjuster_id")
)

claim_side_2 = (
    df_claims
    .filter(F.col("status") == "OPEN")
    .select(F.col("policy_id").alias("adjuster_id"))
)

result = claim_side.subtract(claim_side_2)
