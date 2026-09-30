from pyspark.sql import functions as F

valid = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
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
        F.when(F.col("approved_amount") >= 15477.7, "A").when(F.col("approved_amount") >= 0.0, "B").otherwise("C").alias("approved_amount_tier"),
    )
)

claim_rows = (
    valid
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("claim_date") < F.lit("2024-12-22").cast("date")))
    .select("policy_id", F.col("id").alias("entity_id"), "claimed_amount")
)

claim_side = (
    df_claims
    .select(
        F.col("id").alias("policy_id"),
        F.col("adjuster_id").alias("entity_id"),
        "claimed_amount",
    )
)

result = (
    claim_rows
    .union(claim_side)
    .orderBy("policy_id")
)
