from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter(F.col("approved_amount") <= 17759.3)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

claim_type_stats = (
    prepared
    .groupBy("claim_type")
    .agg(F.avg("claimed_amount").alias("avg_claimed_amount"))
)

vs_group = (
    prepared.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.claimed_amount") > F.col("g.avg_claimed_amount"))
    .select(
        "s.*",
        "g.avg_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.avg_claimed_amount"), 2).alias("claimed_amount_minus_avg"),
    )
)

with_children_claims = (
    vs_group.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.avg_claimed_amount",
        "s.claimed_amount_minus_avg",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

result = (
    with_children_claims
    .groupBy("claim_date", "status")
    .pivot("claim_type", ["flood", "theft", "fire"])
    .agg(F.sum("avg_claimed_amount").alias("amount"), F.count("avg_claimed_amount").alias("n"))
)
