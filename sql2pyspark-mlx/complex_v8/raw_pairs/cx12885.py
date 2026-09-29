from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .select(
        F.lit("claim").alias("record_type"),
        "adjuster_id",
        F.col("approved_amount").alias("amount"),
        "status",
    )
)

claim_side = (
    df_claims
    .filter((F.col("claimed_amount") <= 41878.5) & (F.col("policy_id").isNotNull()))
    .select(
        F.lit("claim_2").alias("record_type"),
        "id",
        "approved_amount",
        "status",
    )
)

result = (
    claim_rows
    .union(claim_side)
    .orderBy(F.col("amount").desc(), "record_type")
)
