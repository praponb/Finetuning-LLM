from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter((F.col("adjuster_id") >= 3) & (F.col("claim_date").between(F.lit("2024-04-14").cast("date"), F.lit("2025-11-29").cast("date"))))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

summary = (
    recent_claims
    .filter(F.col("approved_amount").between(0.0, 25402.3))
    .groupBy("policy_id")
    .agg(
        F.sum("approved_amount").alias("total_approved_amount"),
        F.round(100 * F.count(F.when(F.col("status").isin("APPROVED", "REJECTED"), 1)) / F.count("*"), 1).alias("pct_status_in_approved_rejected"),
        F.round(100 * F.count(F.when(F.col("status") == "IN_REVIEW", 1)) / F.count("*"), 1).alias("pct_status_in_review"),
    )
    .filter(F.col("total_approved_amount") > 0.0)
)

with_parent_policy = (
    summary.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

result = (
    with_parent_policy
    .select(
        "policy_id",
        "total_approved_amount",
        "pct_status_in_approved_rejected",
        "pct_status_in_review",
        (F.floor(F.col("total_approved_amount") / 50000) * 50000).alias("total_approved_amount_bucket"),
        (F.floor(F.col("total_approved_amount") / 50000) * 50000).alias("total_approved_amount_bucket_2"),
    )
    .orderBy(F.col("total_approved_amount_bucket").desc(), "total_approved_amount")
)
