from pyspark.sql import functions as F

matched = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.claim_type",
        "c.status",
        "p.product",
        "p.premium",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(~F.col("status").isin("APPROVED", "IN_REVIEW"), 1)).alias("claim_status_count"),
        F.min("reported_at").alias("first_claim_reported_at"),
        F.count("*").alias("claim_count"),
    )
)

profiled = (
    matched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.first_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

tagged = (
    profiled
    .filter(F.col("claim_date").between(F.lit("2024-06-26").cast("date"), F.lit("2025-09-19").cast("date")))
    .select(
        "*",
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
        (F.col("claim_count") + F.col("premium")).alias("claim_count_plus_premium"),
    )
)

per_group = (
    tagged
    .filter(F.col("premium") > 1132.1)
    .groupBy(F.to_date("claim_date_week").alias("claim_date_week_day"), "status")
    .agg(
        F.round(100 * F.count(F.when(F.col("status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_status_is_not_null"),
        F.count("*").alias("row_count"),
    )
)

result = (
    per_group
    .select(
        "claim_date_week_day",
        "status",
        "pct_status_is_not_null",
        "row_count",
        F.coalesce(F.col("pct_status_is_not_null"), F.lit(0.0)).alias("pct_status_is_not_null_filled"),
        (F.col("row_count") + F.col("pct_status_is_not_null")).alias("row_count_plus_pct_status_is_not_null"),
    )
    .orderBy("claim_date_week_day", "status")
)
