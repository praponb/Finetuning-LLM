from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
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
        F.abs(F.col("approved_amount") - 20843.1).alias("approved_amount_distance"),
        F.concat_ws("|", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

rollup_base = (
    cleaned
    .groupBy(
        F.date_trunc("month", "reported_at").alias("reported_at_month"),
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("status") == "PAID", 1)) / F.count("*"), 1).alias("pct_status_paid"),
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
    )
    .filter(F.col("row_count") >= 3)
)

labeled = (
    rollup_base
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("reported_at_month")).alias("days_since_reported_at_month"),
        F.round(F.col("pct_status_paid") * 0.9, 2).alias("pct_status_paid_adj"),
        (F.floor(F.col("row_count") / 2) * 2).alias("row_count_bucket"),
    )
)

without_matches = (
    labeled
    .crossJoin(labeled.agg(F.avg("pct_status_paid_adj").alias("overall_avg_pct_status_paid_adj")))
    .filter(F.col("pct_status_paid_adj") > F.col("overall_avg_pct_status_paid_adj"))
    .drop("overall_avg_pct_status_paid_adj")
)

labeled_2 = (
    without_matches
    .select(
        "*",
        F.array_join(F.array_sort(F.col("claim_type_list")), ",").alias("claim_type_list_text"),
    )
)

result = (
    labeled_2
    .select(
        "reported_at_month",
        "claim_date_ym",
        "days_since_reported_at_month",
        "row_count",
    )
    .unpivot(
        ids=["reported_at_month", "claim_date_ym"],
        values=["days_since_reported_at_month", "row_count"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
