from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.col("claim_date").between(F.lit("2024-02-29").cast("date"), F.lit("2025-08-10").cast("date"))) & (F.col("policy_id") <= 20))
    .select(
        F.lit("claim").alias("record_type"),
        F.col("status").alias("label"),
        "claimed_amount",
        "reported_at",
    )
)

claim_side = (
    df_claims
    .select(
        F.lit("claim_2").alias("record_type"),
        "status",
        "approved_amount",
        "reported_at",
    )
)

result = claim_rows.union(claim_side)
