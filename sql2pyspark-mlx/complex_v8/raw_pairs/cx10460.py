from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
    )
)

with_children = (
    recent_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

grouped = (
    with_children
    .filter(F.col("claim_id") <= 32)
    .groupBy(
        F.date_trunc("month", "claim_claim_date").alias("claim_claim_date_month"),
        "adjuster_id",
    )
    .agg(
        F.max("claim_approved_amount").alias("max_claim_approved_amount"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.max("claimed_amount").alias("max_claimed_amount"),
        F.min("claim_reported_at").alias("first_claim_reported_at"),
    )
)

result = (
    grouped
    .filter(F.col("max_claim_approved_amount").between(2500.0, 45000.0))
    .select(
        "claim_claim_date_month",
        "adjuster_id",
        "max_claim_approved_amount",
        "distinct_adjuster_ids",
        "max_claimed_amount",
        "first_claim_reported_at",
        (F.floor(F.col("distinct_adjuster_ids") / 2) * 2).alias("distinct_adjuster_ids_bucket"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("claim_claim_date_month")).alias("days_since_claim_claim_date_month"),
    )
    .orderBy(
        F.col("claim_claim_date_month").desc(),
        F.col("max_claim_approved_amount").asc_nulls_last(),
    )
)
