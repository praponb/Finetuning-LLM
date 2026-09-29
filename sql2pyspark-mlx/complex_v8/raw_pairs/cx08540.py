from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .filter((F.col("claimed_amount") > 12726.7) | (F.col("status") == "OPEN"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
    )
)

claim_side = (
    df_claims
    .filter(F.col("approved_amount") < 33690.0)
    .select(
        F.col("claim_type").alias("name"),
        "policy_id",
        F.col("reported_at").alias("happened_at"),
        F.lit("claim").alias("source"),
    )
)

claim_side_2 = (
    df_claims
    .select(
        "claim_type",
        "policy_id",
        "reported_at",
        F.lit("claim_2").alias("source"),
    )
)

result = (
    claim_side
    .union(claim_side_2)
    .distinct()
)
