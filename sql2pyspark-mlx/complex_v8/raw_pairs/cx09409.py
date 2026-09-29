from pyspark.sql import functions as F

scoped = (
    df_claims
    .filter((F.col("claim_date").between(F.lit("2024-02-19").cast("date"), F.lit("2025-08-10").cast("date"))) & (F.col("status").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        F.date_trunc("month", F.col("claim_date")).alias("claim_date_month"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

with_details_policy = (
    scoped.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.claim_type_or_unknown",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type").isin("fire", "collision"), 1)).alias("claim_claim_type_count"),
    )
)

profiled = (
    with_details_policy.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

scored = (
    profiled
    .filter(F.col("policy_start_date") >= F.lit("2024-12-03").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "claim_type_or_unknown",
        "policy_end_date",
        "claim_claim_type_count",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.abs(F.col("policy_premium") - 2096.6).alias("policy_premium_distance"),
    )
)

joined_policy = (
    scored.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.premium").between(1541.8, 4906.3))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.policy_end_date",
        "s.days_since_claim_date",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.premium").alias("policy_premium"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("status") == "IN_REVIEW", 1)).alias("claim_status_count"),
        F.count("*").alias("claim_count"),
        F.sum("claimed_amount").alias("total_claim_claimed_amount"),
    )
)

profiled_2 = (
    joined_policy.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.total_claim_claimed_amount",
    )
)

result = (
    profiled_2
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "policy_end_date",
        "days_since_claim_date",
        "policy_premium",
        "claim_status_count",
        "claim_count",
        F.least(F.col("days_since_claim_date"), F.col("claim_status_count")).alias("least_days_since_claim_date_claim_status_count"),
        F.coalesce(F.col("claim_status_count"), F.lit(0)).alias("claim_status_count_filled"),
    )
    .orderBy("adjuster_id", "id")
    .limit(10)
)
