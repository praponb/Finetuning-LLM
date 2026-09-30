from pyspark.sql import functions as F

valid_claims = (
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
        F.substring(F.trim(F.col("claim_type")), 1, 2).alias("claim_type_prefix"),
    )
)

claim_side = (
    valid_claims
    .filter((F.col("claim_type").isin("theft", "delay")) & (F.col("claim_type_prefix").isNotNull()))
    .select(
        F.col("id").alias("qty"),
        "claim_date",
        F.col("reported_at").alias("happened_at"),
    )
)

claim_rows = (
    df_claims
    .filter(F.col("approved_amount") > 0.0)
    .select(
        F.col("id").alias("qty"),
        "claim_date",
        F.col("reported_at").alias("happened_at"),
    )
)

result = claim_side.union(claim_rows)
