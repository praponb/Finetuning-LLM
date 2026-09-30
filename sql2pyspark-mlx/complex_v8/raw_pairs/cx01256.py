from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_claim_date_ties = Window.partitionBy("status").orderBy("claim_date")
w_status_by_claim_date_full = Window.partitionBy("status").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_claim_date_running = Window.partitionBy("status").orderBy(F.col("claim_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_adjuster_id = Window.partitionBy(F.month("claim_date"), "adjuster_id").orderBy("reported_at_plus_30m")
w_claim_date_month_adjuster_id_2 = Window.partitionBy(F.month("claim_date"), "adjuster_id").orderBy("reported_at_plus_30m", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_claim_date_full = Window.partitionBy("policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_claim_date = Window.partitionBy("status").orderBy("claim_date", "id")
w_claim_date_month_adjuster_id_3 = Window.partitionBy(F.month("claim_date"), "adjuster_id").orderBy("reported_at_plus_30m", "id")
w_second_reported_at_plus_30m = Window.partitionBy(F.to_date("second_reported_at_plus_30m"), F.year("second_reported_at_plus_30m_2"))
w_second_reported_at_plus_30m_2 = Window.partitionBy(F.to_date("second_reported_at_plus_30m_2"), "rn").orderBy("second_claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_second_reported = Window.partitionBy("policy_id").orderBy("second_reported_at_plus_30m", "id")

prepared_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        (F.col("reported_at") + F.expr("INTERVAL 30 MINUTES")).alias("reported_at_plus_30m"),
    )
)

windowed = (
    prepared_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "reported_at_plus_30m",
        F.round(F.cume_dist().over(w_status_by_claim_date_ties), 3).alias("claim_date_cume_dist"),
        F.nth_value("reported_at_plus_30m", 2).over(w_status_by_claim_date_full).alias("second_reported_at_plus_30m"),
        F.first("claim_date", ignorenulls=True).over(w_status_by_claim_date_running).alias("first_known_claim_date"),
        F.round(F.cume_dist().over(w_claim_date_month_adjuster_id), 3).alias("reported_at_plus_30m_cume_dist"),
        F.nth_value("claim_date", 2).over(w_claim_date_month_adjuster_id_2).alias("second_claim_date"),
        F.nth_value("reported_at_plus_30m", 2).over(w_policy_id_by_claim_date_full).alias("second_reported_at_plus_30m_2"),
        F.round(F.percent_rank().over(w_status_by_claim_date_ties), 3).alias("claim_date_percent_rank"),
        F.row_number().over(w_status_by_claim_date).alias("rn"),
        F.round((F.unix_timestamp("reported_at_plus_30m") - F.unix_timestamp(F.lag("reported_at_plus_30m").over(w_claim_date_month_adjuster_id_3))) / 60, 1).alias("minutes_since_prev_reported_at_plus_30m"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "reported_at_plus_30m",
        "claim_date_cume_dist",
        "second_reported_at_plus_30m",
        "first_known_claim_date",
        "second_claim_date",
        "second_reported_at_plus_30m_2",
        "claim_date_percent_rank",
        "rn",
        "minutes_since_prev_reported_at_plus_30m",
        F.round(F.col("rn") - F.avg("rn").over(w_second_reported_at_plus_30m), 2).alias("rn_vs_second_reported_at_plus_30m_to_date_second"),
        F.size(F.collect_set("policy_id").over(w_second_reported_at_plus_30m)).alias("distinct_policy_id_in_second_reported_at_plus_30"),
        F.sum("claim_date_cume_dist").over(w_second_reported_at_plus_30m_2).alias("running_claim_date_cume_dist"),
        F.lead("claim_date_cume_dist", 1, 0).over(w_policy_id_by_second_reported).alias("next_claim_date_cume_dist"),
        F.round((F.unix_timestamp("second_reported_at_plus_30m") - F.unix_timestamp(F.lag("second_reported_at_plus_30m").over(w_policy_id_by_second_reported))) / 60, 1).alias("minutes_since_prev_second_reported_at_plus_30m"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "reported_at_plus_30m",
        "claim_date_cume_dist",
        "second_reported_at_plus_30m",
        "first_known_claim_date",
        "second_claim_date",
        "second_reported_at_plus_30m_2",
        "claim_date_percent_rank",
        "rn",
        "minutes_since_prev_reported_at_plus_30m",
        "rn_vs_second_reported_at_plus_30m_to_date_second",
        "distinct_policy_id_in_second_reported_at_plus_30",
        "running_claim_date_cume_dist",
        "next_claim_date_cume_dist",
        "minutes_since_prev_second_reported_at_plus_30m",
    )
    .orderBy(F.col("second_claim_date").desc())
)
