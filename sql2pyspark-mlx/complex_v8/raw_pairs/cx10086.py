from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_or_unknown_reported_a = Window.partitionBy("status_or_unknown", F.to_date("reported_at_month"))
w_all_by_total_avg_claim_appro = Window.orderBy("total_avg_claim_approved_amount", "reported_at_month", "status_or_unknown")
w_row_count_n_status_in_review = Window.partitionBy("row_count", "n_status_in_review").orderBy("row_count", "reported_at_month", "status_or_unknown").rowsBetween(-3, -1)
w_all_by_total_avg_claim_appro_2 = Window.orderBy("total_avg_claim_approved_amount", "reported_at_month", "status_or_unknown").rowsBetween(-1, -1)

prepared = (
    df_claims
    .filter(F.col("claim_type") == "theft")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        "reported_at",
        F.round(F.col("approved_amount") * 1.5, 2).alias("approved_amount_adj"),
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.sum("claimed_amount").alias("total_claim_claimed_amount"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"),
    )
)

profiled = (
    prepared.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.total_claim_claimed_amount",
        "c.avg_claim_approved_amount",
        "c.avg_claim_claimed_amount",
    )
)

derived = (
    profiled
    .filter(F.col("reported_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp"))
    .withColumn("status_group", F.when(F.col("status").isin("PAID", "IN_REVIEW"), "group_a").when(F.col("status") == "OPEN", "group_b").otherwise("other"))
)

tagged = (
    derived
    .filter(F.col("avg_claim_claimed_amount") >= 5090.0)
    .select(
        "*",
        (F.col("approved_amount_adj") - F.col("avg_claim_claimed_amount")).alias("approved_amount_adj_minus_avg_claim_claimed_amou"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

summary = (
    tagged
    .groupBy(
        F.date_trunc("month", "reported_at").alias("reported_at_month"),
        "status_or_unknown",
    )
    .agg(
        F.min("avg_claim_claimed_amount").alias("min_avg_claim_claimed_amount"),
        F.count("*").alias("row_count"),
        F.round(F.sum("avg_claim_claimed_amount") / F.nullif(F.sum("total_claim_claimed_amount"), F.lit(0)), 3).alias("avg_claim_claimed_amount_to_total_claim_claimed_"),
        F.sum(F.when(F.col("status") == "IN_REVIEW", 1).otherwise(0)).alias("n_status_in_review"),
        F.round(F.sum("avg_claim_approved_amount"), 2).alias("total_avg_claim_approved_amount"),
    )
    .filter(F.col("row_count") >= 2)
)

with_windows = (
    summary
    .select(
        "reported_at_month",
        "status_or_unknown",
        "min_avg_claim_claimed_amount",
        "avg_claim_claimed_amount_to_total_claim_claimed_",
        "n_status_in_review",
        "total_avg_claim_approved_amount",
        F.count(F.lit(1)).over(w_status_or_unknown_reported_a).alias("rows_in_status_or_unknown_reported_at_month_to_d"),
        F.max("n_status_in_review").over(w_status_or_unknown_reported_a).alias("status_or_unknown_reported_at_month_to_date_max_"),
        F.round((F.unix_timestamp("reported_at_month") - F.unix_timestamp(F.lag("reported_at_month").over(w_all_by_total_avg_claim_appro))) / 60, 1).alias("minutes_since_prev_reported_at_month"),
        F.round(F.avg("row_count").over(w_row_count_n_status_in_review), 2).alias("row_count_avg_3_prior"),
        F.sum("row_count").over(w_all_by_total_avg_claim_appro_2).alias("row_count_sum_1_prior"),
    )
)

derived_2 = (
    with_windows
    .select(
        "*",
        F.round(F.col("row_count_avg_3_prior") * 1.1, 2).alias("row_count_avg_3_prior_adj"),
        F.date_trunc("hour", F.col("reported_at_month")).alias("reported_at_month_hour"),
        F.lower(F.regexp_replace(F.trim(F.col("status_or_unknown")), " ", "_")).alias("status_or_unknown_slug"),
    )
)

result = (
    derived_2
    .filter(F.col("row_count_avg_3_prior") < 10.9)
    .select(
        "reported_at_month",
        "avg_claim_claimed_amount_to_total_claim_claimed_",
        "n_status_in_review",
        "status_or_unknown_reported_at_month_to_date_max_",
        "minutes_since_prev_reported_at_month",
        "row_count_sum_1_prior",
        "row_count_avg_3_prior_adj",
        "reported_at_month_hour",
        F.abs(F.col("n_status_in_review") - 4).alias("n_status_in_review_distance"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_at_month")).alias("days_since_reported_at_month"),
    )
    .orderBy(F.col("n_status_in_review").desc(), "row_count_sum_1_prior")
)
