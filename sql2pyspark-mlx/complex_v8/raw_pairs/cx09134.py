from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        F.col("policy_id").alias("ref_id"),
        F.col("id").alias("entity_id"),
        "status",
        F.lit("claim").alias("origin"),
    )
)

claim_side_2 = (
    df_claims
    .select(
        "policy_id",
        "adjuster_id",
        "status",
        F.lit("claim_2").alias("origin"),
    )
)

result = (
    claim_side
    .union(claim_side_2)
    .orderBy("status")
)
