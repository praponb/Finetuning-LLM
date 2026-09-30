from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter((F.col("claimed_amount") > 18478.3) & (F.col("status") != "PAID"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
        F.date_format(F.col("reported_at"), "yyyy-MM-dd").alias("reported_at_label"),
    )
)

combined_details = (
    recent_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.status",
        "s.reported_at",
        "s.claimed_amount_per_approved_amount",
        "s.claim_date_month_end",
        F.col("p.start_date").alias("policy_start_date"),
    )
)

stats = (
    combined_details
    .filter(F.col("claimed_amount_per_approved_amount").between(3.0, 8.5))
    .groupBy(
        F.date_trunc("month", "policy_start_date").alias("policy_start_date_month"),
    )
    .agg(
        F.percentile_approx("claimed_amount_per_approved_amount", 0.5).alias("median_claimed_amount_per_approved_amount"),
        F.round(100 * F.count(F.when(~F.col("status").isin("OPEN", "IN_REVIEW"), 1)) / F.count("*"), 1).alias("pct_status_not_in_open_in_review"),
        F.count("*").alias("cnt"),
        F.round(F.sum("claimed_amount_per_approved_amount"), 2).alias("total_claimed_amount_per_approved_amount"),
    )
)

without_matches = (
    stats
    .crossJoin(stats.agg(F.avg("total_claimed_amount_per_approved_amount").alias("overall_avg_total_claimed_amount_per_approved_am")))
    .filter(F.col("total_claimed_amount_per_approved_amount") > F.col("overall_avg_total_claimed_amount_per_approved_am"))
    .drop("overall_avg_total_claimed_amount_per_approved_am")
)

annotated = (
    without_matches
    .filter(F.col("pct_status_not_in_open_in_review") >= 30.0)
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("policy_start_date_month")).alias("days_since_policy_start_date_month"),
        F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"),
    )
)

filtered = (
    annotated
    .crossJoin(annotated.agg(F.avg("median_claimed_amount_per_approved_amount").alias("overall_avg_median_claimed_amount_per_approved_a")))
    .filter(F.col("median_claimed_amount_per_approved_amount") > F.col("overall_avg_median_claimed_amount_per_approved_a"))
    .drop("overall_avg_median_claimed_amount_per_approved_a")
)

result = (
    filtered
    .filter(F.col("cnt") <= 11)
    .select(
        "policy_start_date_month",
        "median_claimed_amount_per_approved_amount",
        "pct_status_not_in_open_in_review",
        "cnt",
        "total_claimed_amount_per_approved_amount",
        "days_since_policy_start_date_month",
        (F.col("cnt") + F.col("cnt_filled")).alias("cnt_plus_cnt_filled"),
        F.date_trunc("week", F.col("policy_start_date_month")).alias("policy_start_date_month_week"),
    )
    .orderBy(
        F.col("cnt_plus_cnt_filled").asc_nulls_last(),
        "policy_start_date_month",
    )
    .limit(20)
)
