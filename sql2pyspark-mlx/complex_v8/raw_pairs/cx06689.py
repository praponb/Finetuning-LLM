from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter(F.col("claim_date").between(F.lit("2024-02-23").cast("date"), F.lit("2025-11-12").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.count(F.when(F.col("claim_type") != "delay", 1)).alias("claim_claim_type_count"),
        F.min("reported_at").alias("first_claim_reported_at"),
    )
)

with_stats = (
    base_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.min_claim_claimed_amount",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.first_claim_reported_at",
    )
)

with_parent = (
    with_stats.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.reported_at_date",
        "s.status_prefix",
        "s.min_claim_claimed_amount",
        "s.claim_claim_type_count",
        "s.first_claim_reported_at",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        F.col("p.status").alias("policy_status"),
        F.col("p.premium").alias("policy_premium"),
    )
)

stats = (
    with_parent
    .groupBy(
        F.date_trunc("month", "reported_at_date").alias("reported_at_date_month"),
        "claim_claim_type_count",
    )
    .agg(
        F.max("min_claim_claimed_amount").alias("max_min_claim_claimed_amount"),
        F.sum(F.when(F.col("claim_type") == "collision", 1).otherwise(0)).alias("n_claim_type_collision"),
        F.round(F.stddev("claim_claim_type_count"), 2).alias("stddev_claim_claim_type_count"),
        F.avg("adjuster_level").alias("avg_adjuster_level_check"),
    )
    .filter(F.col("avg_adjuster_level_check") > 2)
    .drop("avg_adjuster_level_check")
)

result = (
    stats
    .select("stddev_claim_claim_type_count")
    .distinct()
    .orderBy(F.col("stddev_claim_claim_type_count").asc_nulls_last())
)
