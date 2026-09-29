from pyspark.sql import functions as F

recent = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-02-06 07:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

claim_side = (
    df_claims
    .select(
        F.col("policy_id").alias("entity_id"),
        F.col("approved_amount").alias("value"),
        F.col("reported_at").alias("event_time"),
        F.lit("claim").alias("source"),
    )
)

claim_rows = (
    df_claims
    .filter((F.col("claimed_amount") <= 38909.4) & (F.col("reported_at") < F.lit("2025-03-08 14:00:00").cast("timestamp")))
    .select(
        "policy_id",
        "approved_amount",
        "reported_at",
        F.lit("claim_2").alias("source"),
    )
)

result = claim_side.union(claim_rows)
