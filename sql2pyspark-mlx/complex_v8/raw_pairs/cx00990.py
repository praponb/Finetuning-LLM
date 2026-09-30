from pyspark.sql import functions as F

scoped_claims = (
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

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("status") == "PAID", 1)).alias("claim_status_count"),
        F.count("*").alias("claim_count_2"),
    )
)

with_counts = (
    scoped_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

labeled = (
    with_counts
    .filter(F.col("claimed_amount").between(3192.1, 45826.9))
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        "claim_count",
        "claim_status_count",
        "claim_count_2",
        (F.col("reported_at") + F.expr("INTERVAL 30 MINUTES")).alias("reported_at_plus_30m"),
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
        F.trunc(F.col("claim_date"), "MM").alias("claim_date_month_start"),
    )
)

result = (
    labeled
    .groupingSets(
        [["adjuster_id", "claim_count"], ["adjuster_id"], []],
        "adjuster_id", "claim_count",
    )
    .agg(
        F.grouping("adjuster_id").alias("is_total_adjuster_id"),
        F.count("*").alias("row_count"),
        F.percentile_approx("approved_amount", 0.5).alias("median_approved_amount"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("claim_count").asc_nulls_last(),
    )
)
