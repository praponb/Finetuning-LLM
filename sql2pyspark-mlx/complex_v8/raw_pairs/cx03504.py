from pyspark.sql import functions as F

recent = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(9, 11)) & (F.col("approved_amount") < 23207.7))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.abs(F.col("approved_amount") - 4554.8).alias("approved_amount_distance"),
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

with_children_claims = (
    recent.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

adjuster_id_stats = (
    with_children_claims
    .groupBy("adjuster_id")
    .agg(F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"))
)

above_average = (
    with_children_claims.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.claimed_amount") >= F.col("g.median_claimed_amount"))
    .select(
        "s.*",
        "g.median_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.median_claimed_amount"), 2).alias("claimed_amount_minus_median"),
    )
)

labeled = (
    above_average
    .filter(F.col("claim_type").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        "claim_id",
        "claim_status",
        "median_claimed_amount",
        "claimed_amount_minus_median",
        F.hour(F.col("reported_at")).alias("reported_at_hour"),
    )
)

result = (
    labeled
    .groupBy("reported_at_hour", "policy_id")
    .pivot("claim_type", ["delay", "collision"])
    .agg(F.avg("claimed_amount_minus_median"))
)
