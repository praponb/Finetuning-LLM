from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.col("policy_id") >= 20) & (F.col("status") == "REJECTED"))
    .select(
        F.lit("claim").alias("record_type"),
        F.col("policy_id").alias("entity_id"),
        "adjuster_id",
        "reported_at",
    )
)

claim_side_2 = (
    df_claims
    .filter((F.col("status").isin("IN_REVIEW", "PAID")) & (F.col("claimed_amount") > 12726.7))
    .select(
        F.lit("claim_2").alias("record_type"),
        "policy_id",
        "adjuster_id",
        "reported_at",
    )
)

result = (
    claim_side
    .union(claim_side_2)
    .orderBy(F.col("record_type").desc())
)
