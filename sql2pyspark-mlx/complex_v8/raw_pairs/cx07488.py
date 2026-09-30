from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

scored = (
    recent_claims
    .filter(F.col("claim_type_clean").isNotNull())
    .select(
        "id",
        "policy_id",
        "claim_type",
        "approved_amount",
        "claimed_amount_filled",
        "status_prefix",
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
        F.substring(F.trim(F.col("claim_type")), 1, 2).alias("claim_type_prefix"),
    )
)

result = (
    scored
    .rollup("policy_id", "claim_type")
    .agg(
        F.max("approved_amount_bucket").alias("max_approved_amount_bucket"),
        F.count("*").alias("row_count"),
        F.round(F.sum("claimed_amount_filled") / F.nullif(F.sum("approved_amount"), F.lit(0)), 3).alias("claimed_amount_filled_to_approved_amount_ratio"),
    )
    .select(
        "policy_id",
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "max_approved_amount_bucket",
        "row_count",
        "claimed_amount_filled_to_approved_amount_ratio",
    )
)
