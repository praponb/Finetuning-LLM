from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter((F.col("status") != "REJECTED") & (F.col("claim_date").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.least(F.col("claimed_amount"), F.col("approved_amount")).alias("least_claimed_amount_approved_amount"),
        F.month(F.col("reported_at")).alias("reported_at_month"),
        F.abs(F.col("claimed_amount") - 29336.0).alias("claimed_amount_distance"),
    )
)

enriched = (
    cleaned
    .filter(F.col("claim_type").isin("medical", "theft", "delay"))
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "least_claimed_amount_approved_amount",
        "reported_at_month",
        "claimed_amount_distance",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.abs(F.col("reported_at_month") - 6).alias("reported_at_month_distance"),
        F.date_trunc("week", F.col("reported_at")).alias("reported_at_week"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.min("approved_amount").alias("min_claim_approved_amount"),
        F.count("*").alias("claim_count"),
    )
)

with_counts = (
    enriched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.first_claim_date",
        "c.min_claim_approved_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

result = (
    with_counts
    .groupBy("reported_at_month_distance")
    .pivot("claim_type", ["delay", "medical", "fire"])
    .agg(F.count("claimed_amount"))
)
