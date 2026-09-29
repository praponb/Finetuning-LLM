from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter(F.month(F.col("reported_at")).isin(1, 2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
    )
)

claim_side = (
    cleaned_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.year(F.col("claim_date")) == 2025))
    .select(
        F.lit("claim").alias("record_type"),
        "id",
        "approved_amount",
        F.col("claim_type").alias("label"),
    )
)

claim_side_2 = (
    df_claims
    .select(
        F.lit("claim_2").alias("record_type"),
        "adjuster_id",
        "claimed_amount",
        "status",
    )
)

result = claim_side.union(claim_side_2)
