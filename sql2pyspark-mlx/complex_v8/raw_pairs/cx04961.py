from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_policy_id_b = Window.partitionBy(F.month("claim_date"), "policy_id").orderBy(F.col("claim_date").desc(), "id")
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_claim_type_by_claim_date = Window.partitionBy("claim_type").orderBy(F.col("claim_date").desc(), "id")
w_claim_type_policy_id_by_clai = Window.partitionBy("claim_type", "policy_id").orderBy(F.col("claim_date").desc(), "id")
w_claim_date_year_adjuster_id = Window.partitionBy(F.year("claim_date"), "adjuster_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_month_policy_id_b_2 = Window.partitionBy(F.month("claim_date"), "policy_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_claim_date_ties = Window.partitionBy("policy_id").orderBy("claim_date")
w_policy_id = Window.partitionBy("policy_id")

cleaned = (
    df_claims
    .filter(F.col("approved_amount") >= 0.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "reported_at",
        F.lead("reported_at", 3).over(w_claim_date_month_policy_id_b).alias("next3_reported_at"),
        F.lead("claim_date").over(w_all_by_reported_at).alias("next_claim_date"),
        F.rank().over(w_claim_type_by_claim_date).alias("claim_date_rank"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_claim_date_month_policy_id_b))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.round(F.percent_rank().over(w_claim_type_policy_id_by_clai), 3).alias("claim_date_percent_rank"),
        F.nth_value("claim_date", 2).over(w_claim_date_year_adjuster_id).alias("second_claim_date"),
        F.lag("reported_at", 2).over(w_claim_type_by_claim_date).alias("prev2_reported_at"),
        F.row_number().over(w_claim_type_policy_id_by_clai).alias("claim_date_seq"),
        F.last("claim_date").over(w_claim_date_month_policy_id_b_2).alias("last_claim_date"),
    )
)

labeled = (
    with_windows
    .select(
        "*",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_ref"),
        (F.col("prev2_reported_at") + F.expr("INTERVAL 15 MINUTES")).alias("prev2_reported_at_plus_15m"),
        F.abs(F.col("claim_date_percent_rank") - 0.5).alias("claim_date_percent_rank_distance"),
    )
)

metrics = (
    labeled
    .select(
        "id",
        "claim_date",
        "reported_at",
        "next3_reported_at",
        "next_claim_date",
        "minutes_since_prev_reported_at",
        "claim_date_percent_rank",
        "second_claim_date",
        "prev2_reported_at",
        "claim_date_seq",
        "last_claim_date",
        "claim_type_ref",
        "prev2_reported_at_plus_15m",
        F.round(F.percent_rank().over(w_policy_id_by_claim_date_ties), 3).alias("claim_date_percent_rank_2"),
        F.min("claim_date_seq").over(w_policy_id).alias("policy_id_min_claim_date_seq"),
        F.round(F.col("claim_date_percent_rank") - F.avg("claim_date_percent_rank").over(w_policy_id), 2).alias("claim_date_percent_rank_vs_policy_id_avg"),
        F.round(F.col("claim_date_percent_rank") / F.nullif(F.sum("claim_date_percent_rank").over(w_policy_id), F.lit(0)), 4).alias("claim_date_percent_rank_share_of_policy_id"),
    )
)

rollup_base = (
    metrics
    .filter(F.col("claim_type_ref").isNotNull())
    .groupBy(
        F.date_trunc("month", "last_claim_date").alias("last_claim_date_month"),
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
        F.date_trunc("month", "prev2_reported_at_plus_15m").alias("prev2_reported_at_plus_15m_month"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("policy_id_min_claim_date_seq") / F.nullif(F.sum("claim_date_percent_rank_vs_policy_id_avg"), F.lit(0)), 3).alias("policy_id_min_claim_date_seq_to_claim_date_perce"),
        F.min("claim_date_seq").alias("min_claim_date_seq"),
        F.sum("claim_date_percent_rank_vs_policy_id_avg").alias("sum_claim_date_percent_rank_vs_policy_id_avg_che"),
    )
    .filter(F.col("sum_claim_date_percent_rank_vs_policy_id_avg_che") > -0.7)
    .drop("sum_claim_date_percent_rank_vs_policy_id_avg_che")
)

result = (
    rollup_base
    .select(
        "last_claim_date_month",
        "prev2_reported_at_plus_15m_month",
        "n_rows",
        "distinct_ids",
        "policy_id_min_claim_date_seq_to_claim_date_perce",
        "min_claim_date_seq",
    )
    .orderBy(
        F.col("prev2_reported_at_plus_15m_month").desc(),
        F.col("distinct_ids").asc_nulls_last(),
    )
)
