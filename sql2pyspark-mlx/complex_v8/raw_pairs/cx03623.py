from pyspark.sql import functions as F

scoped = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

policy_id_stats = (
    scoped
    .groupBy("policy_id")
    .agg(F.percentile_approx("approved_amount", 0.5).alias("median_approved_amount"))
)

vs_group = (
    scoped.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.approved_amount") >= F.col("g.median_approved_amount"))
    .select(
        "s.*",
        "g.median_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.median_approved_amount"), 2).alias("approved_amount_minus_median"),
    )
)

with_children = (
    vs_group.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.median_approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

qualified = (
    with_children
    .crossJoin(with_children.agg(F.avg("claimed_amount").alias("overall_avg_claimed_amount")))
    .filter(F.col("claimed_amount") > F.col("overall_avg_claimed_amount"))
    .drop("overall_avg_claimed_amount")
)

result = (
    qualified
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "median_approved_amount",
        "claim_id",
        "claim_reported_at",
        "claim_status",
        F.lower(F.regexp_replace(F.trim(F.col("claim_status")), " ", "_")).alias("claim_status_slug"),
        F.substring(F.trim(F.col("claim_status")), 1, 4).alias("claim_status_prefix"),
    )
    .orderBy(F.col("median_approved_amount").desc())
)
