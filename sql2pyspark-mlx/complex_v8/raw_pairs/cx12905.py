from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .filter(F.col("approved_amount") >= 0.0)
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
        F.round(F.col("claimed_amount") * 1.5, 2).alias("claimed_amount_adj"),
    )
)

without_matches = (
    scoped_claims
    .crossJoin(scoped_claims.agg(F.max("claimed_amount_adj").alias("overall_max_claimed_amount_adj")))
    .filter(F.col("claimed_amount_adj") < F.col("overall_max_claimed_amount_adj"))
    .drop("overall_max_claimed_amount_adj")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

with_counts = (
    without_matches.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

scored = (
    with_counts
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "claimed_amount_adj",
        "claim_count",
        F.round(F.col("approved_amount") * 1.07, 2).alias("approved_amount_adj"),
        (F.floor(F.col("claimed_amount_adj") / 10000) * 10000).alias("claimed_amount_adj_bucket"),
    )
)

screened = (
    scored.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.status") == "IN_REVIEW"), "left_anti")
)

claim_stats_2 = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.count(F.when(F.col("status").isin("OPEN", "REJECTED", "IN_REVIEW"), 1)).alias("claim_status_count"),
    )
)

with_counts_2 = (
    screened.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.last_claim_reported_at",
        "c.min_claim_claimed_amount",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

labeled = (
    with_counts_2
    .filter(F.col("approved_amount_adj") > 18750.0)
    .select(
        "*",
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.round((F.unix_timestamp(F.col("last_claim_reported_at")) - F.unix_timestamp(F.col("reported_at"))) / 60, 1).alias("minutes_reported_at_to_last_claim_reported_at"),
    )
)

result = (
    labeled
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        "claimed_amount_adj",
        "claim_count",
        "approved_amount_adj",
        "claim_status_count",
        "claim_date_month_end",
        "days_since_claim_date",
        "minutes_reported_at_to_last_claim_reported_at",
        F.greatest(F.col("claim_status_count"), F.col("days_since_claim_date")).alias("greatest_claim_status_count_days_since_claim_dat"),
    )
    .orderBy(
        F.col("adjuster_id").desc(),
        F.col("claim_status_count").desc_nulls_first(),
        "id",
    )
    .limit(20)
)
