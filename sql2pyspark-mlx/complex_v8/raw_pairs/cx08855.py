from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.col("claim_type") == "delay")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.when(F.col("status").isin("PAID", "IN_REVIEW"), "group_a").when(F.col("status") == "APPROVED", "group_b").otherwise("other").alias("status_group"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

totals = (
    cleaned
    .filter(F.col("policy_id") >= 8)
    .groupBy("status_len", F.to_date("reported_at").alias("reported_at_day"))
    .agg(
        F.round(F.sum("approved_amount"), 2).alias("total_approved_amount"),
        F.round(F.sum("days_since_claim_date") / F.nullif(F.sum("status_len"), F.lit(0)), 3).alias("days_since_claim_date_to_status_len_ratio"),
        F.count(F.when(F.col("claim_type") == "collision", 1)).alias("n_claim_type_collision"),
    )
)

scored = (
    totals
    .select(
        "*",
        F.date_add(F.col("reported_at_day"), 7).alias("reported_at_day_plus_7d"),
    )
)

with_activity = (
    scored
    .crossJoin(scored.agg(F.max("days_since_claim_date_to_status_len_ratio").alias("overall_max_days_since_claim_date_to_status_len_")))
    .filter(F.col("days_since_claim_date_to_status_len_ratio") < F.col("overall_max_days_since_claim_date_to_status_len_"))
    .drop("overall_max_days_since_claim_date_to_status_len_")
)

tagged = (
    with_activity
    .select(
        "*",
        F.when(F.col("status_len") >= 11, "A").when(F.col("status_len") >= 7, "B").otherwise("C").alias("status_len_tier"),
        (F.col("total_approved_amount") - F.col("status_len")).alias("total_approved_amount_minus_status_len"),
    )
)

without_matches = (
    tagged
    .crossJoin(tagged.agg(F.avg("total_approved_amount_minus_status_len").alias("overall_avg_total_approved_amount_minus_status_l")))
    .filter(F.col("total_approved_amount_minus_status_len") > F.col("overall_avg_total_approved_amount_minus_status_l"))
    .drop("overall_avg_total_approved_amount_minus_status_l")
)

result = (
    without_matches
    .groupBy("reported_at_day_plus_7d", "reported_at_day")
    .pivot("status_len_tier", ["A", "C", "B"])
    .agg(F.max("total_approved_amount").alias("max_v"), F.count("total_approved_amount").alias("n"))
    .orderBy("reported_at_day_plus_7d", "reported_at_day")
)
