from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.col("claim_type").isin("delay", "theft", "medical"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

labeled = (
    recent_claims
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "approved_amount",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

result = (
    labeled
    .cube("status", "policy_id")
    .agg(
        F.grouping("status").alias("is_total_status"),
        F.countDistinct("status").alias("distinct_status"),
        F.sum(F.when(F.col("status") != "IN_REVIEW", F.col("claimed_amount")).otherwise(0)).alias("claimed_amount_status_in_review"),
        F.round(100 * F.count(F.when(F.col("status") == "REJECTED", 1)) / F.count("*"), 1).alias("pct_status_rejected"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "policy_id",
        "is_total_status",
        "distinct_status",
        "claimed_amount_status_in_review",
        "pct_status_rejected",
    )
    .orderBy(F.col("status").asc_nulls_last(), F.col("policy_id").asc_nulls_last())
)
