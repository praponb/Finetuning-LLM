from pyspark.sql import functions as F

cleaned = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.date_trunc("month", F.col("claim_date")).alias("claim_date_month"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

summarized_rows = (
    cleaned.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

result = (
    summarized_rows
    .cube("adjuster_id", "claim_count", "claim_type_or_unknown")
    .agg(
        F.round(F.avg("claim_count"), 2).alias("avg_claim_count"),
        F.percentile_approx("claim_count", 0.5).alias("median_claim_count"),
    )
    .select(
        "adjuster_id",
        "claim_count",
        F.coalesce(F.col("claim_type_or_unknown"), F.lit("ALL")).alias("claim_type_or_unknown"),
        "avg_claim_count",
        "median_claim_count",
    )
)
