from pyspark.sql import functions as F

recent_claims = (
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
        F.date_add(F.col("claim_date"), 365).alias("claim_date_plus_365d"),
    )
)

claim_side = (
    recent_claims
    .select(
        F.col("adjuster_id").alias("qty"),
        "reported_at",
        "claimed_amount",
        F.lit("claim").alias("record_type"),
    )
)

claim_side_2 = (
    df_claims
    .select(
        "id",
        "reported_at",
        "approved_amount",
        F.lit("claim_2").alias("record_type"),
    )
)

result = claim_side.union(claim_side_2)
