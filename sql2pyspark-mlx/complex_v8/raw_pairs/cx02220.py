from pyspark.sql import functions as F

scoped = (
    df_claims
    .filter((F.col("status") == "REJECTED") & (F.col("claim_date") >= F.lit("2024-08-01").cast("date")))
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
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
    .filter(F.col("claimed_amount") > 12010.5)
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.count("*").alias("claim_count"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

with_stats = (
    without_matches.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.avg_claim_approved_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.first_claim_date",
    )
)

result = (
    with_stats
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "status_slug",
        "avg_claim_approved_amount",
        "claim_count",
        (F.col("approved_amount") - F.col("claim_count")).alias("approved_amount_minus_claim_count"),
    )
)
