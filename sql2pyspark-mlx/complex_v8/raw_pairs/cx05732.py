from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_month_policy_id = Window.partitionBy(F.month("reported_at"), "policy_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_claim_type = Window.partitionBy("status", "claim_type")
w_policy_start_date_year_by_cl = Window.partitionBy(F.year("policy_start_date")).orderBy("claim_date", "id")
w_reported_at_month_policy_id_2 = Window.partitionBy(F.month("reported_at"), "policy_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_month_policy_id_3 = Window.partitionBy(F.month("reported_at"), "policy_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(-6, -1)
w_claim_date_month_policy_star = Window.partitionBy(F.month("claim_date"), F.month("policy_start_date")).orderBy("claim_date_percent_rank", "id")
w_claim_type_by_rows_in_status = Window.partitionBy("claim_type").orderBy("rows_in_status_claim_type", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_policy_id_in_status = Window.partitionBy("distinct_policy_id_in_status_claim_type", F.date_trunc("month", "reported_at")).orderBy(F.col("days_since_prev_policy_start_date").asc_nulls_last(), "id")
w_distinct_policy_id_in_status_2 = Window.partitionBy("distinct_policy_id_in_status_claim_type", "status")
w_claim_date_month_policy_star_2 = Window.partitionBy(F.month("claim_date"), F.month("policy_start_date")).orderBy("claim_date_percent_rank")

base_claims = (
    df_claims
    .filter((F.col("claim_date").isNotNull()) & (F.hour(F.col("reported_at")).between(10, 16)))
)

with_details_policy = (
    base_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select("s.*", F.col("p.start_date").alias("policy_start_date"))
)

windowed = (
    with_details_policy
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        "policy_start_date",
        F.sum("claimed_amount").over(w_reported_at_month_policy_id).alias("running_claimed_amount"),
        F.count(F.lit(1)).over(w_status_claim_type).alias("rows_in_status_claim_type"),
        F.lag("approved_amount").over(w_policy_start_date_year_by_cl).alias("prev_approved_amount"),
        F.nth_value("approved_amount", 2).over(w_reported_at_month_policy_id_2).alias("second_approved_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_status_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_status_claim_type"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status_claim_type), 2).alias("claimed_amount_vs_status_claim_type_avg"),
        F.size(F.collect_set("policy_id").over(w_status_claim_type)).alias("distinct_policy_id_in_status_claim_type"),
        F.datediff(F.col("policy_start_date"), F.lag("policy_start_date").over(w_policy_start_date_year_by_cl)).alias("days_since_prev_policy_start_date"),
        F.max("claimed_amount").over(w_status_claim_type).alias("status_claim_type_max_claimed_amount"),
        F.nth_value("policy_start_date", 2).over(w_reported_at_month_policy_id_2).alias("second_policy_start_date"),
        F.min("claimed_amount").over(w_status_claim_type).alias("status_claim_type_min_claimed_amount"),
        F.round(F.percent_rank().over(w_policy_start_date_year_by_cl), 3).alias("claim_date_percent_rank"),
        F.round(F.avg("claimed_amount").over(w_reported_at_month_policy_id_3), 2).alias("claimed_amount_avg_6_prior"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.row_number().over(w_claim_date_month_policy_star).alias("rn"),
        F.datediff(F.col("policy_start_date"), F.lag("policy_start_date").over(w_claim_date_month_policy_star)).alias("days_since_prev_policy_start_date_2"),
        F.nth_value("second_policy_start_date", 2).over(w_claim_type_by_rows_in_status).alias("second_second_policy_start_date"),
        F.ntile(10).over(w_distinct_policy_id_in_status).alias("days_since_prev_policy_start_date_decile"),
        F.lead("claim_date_percent_rank", 1, 0).over(w_distinct_policy_id_in_status).alias("next_claim_date_percent_rank"),
        F.round(F.col("claim_date_percent_rank") / F.nullif(F.sum("claim_date_percent_rank").over(w_distinct_policy_id_in_status_2), F.lit(0)), 4).alias("claim_date_percent_rank_share_of_distinct_policy"),
        F.round(F.cume_dist().over(w_claim_date_month_policy_star_2), 3).alias("claim_date_percent_rank_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "policy_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        "running_claimed_amount",
        "rows_in_status_claim_type",
        "status_claim_type_max_claimed_amount",
        "status_claim_type_min_claimed_amount",
        "claimed_amount_avg_6_prior",
        "rn",
        "days_since_prev_policy_start_date_2",
        "second_second_policy_start_date",
        "next_claim_date_percent_rank",
        "claim_date_percent_rank_cume_dist",
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
    .orderBy(F.col("claim_type").desc(), F.col("status_prefix").desc())
)
