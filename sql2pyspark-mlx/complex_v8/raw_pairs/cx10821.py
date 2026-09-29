from pyspark.sql import functions as F

filtered = (
    df_claims
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
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

combined_details_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isin("Travel", "Health", "Motor")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.reported_at",
        "s.approved_amount_filled",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"),
        F.min("approved_amount").alias("min_claim_approved_amount"),
    )
)

profiled = (
    combined_details_adjuster.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.avg_claim_claimed_amount",
        "c.min_claim_approved_amount",
    )
)

result = (
    profiled
    .groupBy("adjuster_team")
    .pivot("claim_type", ["delay", "flood"])
    .agg(F.avg("adjuster_level"))
)
