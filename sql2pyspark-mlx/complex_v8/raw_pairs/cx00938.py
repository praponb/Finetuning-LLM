from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at = Window.orderBy(F.col("reported_at").asc_nulls_last(), "id")
w_all_by_reported_at_full = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_month_by_claim_d = Window.partitionBy(F.month("reported_at")).orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_policy_start_date = Window.orderBy("policy_start_date", "id")
w_policy_id_by_policy_start_da = Window.partitionBy("policy_id").orderBy("policy_start_date", "id")
w_policy_start_date_month_adju = Window.partitionBy(F.date_trunc("month", "policy_start_date"), "adjuster_id").orderBy("reported_at", "id")
w_reported_at_month_by_claim_d_2 = Window.partitionBy(F.month("reported_at")).orderBy("claim_date", "id")
w_all_by_policy_start_date_tie = Window.orderBy("policy_start_date")
w_claim_date_rank_next_claim_d = Window.partitionBy("claim_date_rank", F.month("next_claim_date_year"))
w_reported_at_quartile_filled = Window.partitionBy("reported_at_quartile_filled", "policy_start_date_dense_rank")
w_policy_start_date_dense_rank = Window.partitionBy("policy_start_date_dense_rank").orderBy("next_claim_date_year")

recent = (
    df_claims
    .filter((F.col("status").isin("IN_REVIEW", "PAID", "APPROVED")) & (F.col("claimed_amount").between(15381.6, 43500.4)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
    )
)

with_details = (
    recent.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.premium").between(2033.8, 4906.3)), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.reported_at",
        F.col("p.status").alias("policy_status"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

with_trends = (
    with_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "policy_status",
        "policy_start_date",
        F.ntile(4).over(w_all_by_reported_at).alias("reported_at_quartile"),
        F.last("claim_date").over(w_all_by_reported_at_full).alias("last_claim_date"),
        F.nth_value("policy_start_date", 2).over(w_reported_at_month_by_claim_d).alias("second_policy_start_date"),
        F.lead("claim_date", 2).over(w_all_by_policy_start_date).alias("next2_claim_date"),
        F.lead("claim_date").over(w_policy_id_by_policy_start_da).alias("next_claim_date"),
        F.lead("claim_date").over(w_policy_start_date_month_adju).alias("next_claim_date_2"),
        F.rank().over(w_reported_at_month_by_claim_d_2).alias("claim_date_rank"),
        F.dense_rank().over(w_all_by_policy_start_date_tie).alias("policy_start_date_dense_rank"),
    )
)

derived = (
    with_trends
    .select(
        "*",
        F.coalesce(F.col("reported_at_quartile"), F.lit(0)).alias("reported_at_quartile_filled"),
        F.date_trunc("year", F.col("next_claim_date")).alias("next_claim_date_year"),
        (F.floor(F.col("reported_at_quartile") / 2) * 2).alias("reported_at_quartile_bucket"),
    )
)

with_windows = (
    derived
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "policy_status",
        "policy_start_date",
        "last_claim_date",
        "second_policy_start_date",
        "next2_claim_date",
        "next_claim_date_2",
        "claim_date_rank",
        "policy_start_date_dense_rank",
        "reported_at_quartile_filled",
        "reported_at_quartile_bucket",
        F.round(F.col("claim_date_rank") / F.nullif(F.sum("claim_date_rank").over(w_claim_date_rank_next_claim_d), F.lit(0)), 4).alias("claim_date_rank_share_of_claim_date_rank_next_cl"),
        F.count(F.lit(1)).over(w_reported_at_quartile_filled).alias("rows_in_reported_at_quartile_filled_policy_start"),
        F.rank().over(w_policy_start_date_dense_rank).alias("next_claim_date_year_rank"),
        F.round(F.col("reported_at_quartile") - F.avg("reported_at_quartile").over(w_reported_at_quartile_filled), 2).alias("reported_at_quartile_vs_reported_at_quartile_fil"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "policy_status",
        "policy_start_date",
        "last_claim_date",
        "second_policy_start_date",
        "next2_claim_date",
        "next_claim_date_2",
        "claim_date_rank",
        "policy_start_date_dense_rank",
        "reported_at_quartile_filled",
        "reported_at_quartile_bucket",
        "claim_date_rank_share_of_claim_date_rank_next_cl",
        "rows_in_reported_at_quartile_filled_policy_start",
        "next_claim_date_year_rank",
        "reported_at_quartile_vs_reported_at_quartile_fil",
        F.coalesce(F.col("policy_status"), F.lit("UNKNOWN")).alias("policy_status_or_unknown"),
        F.date_trunc("week", F.col("second_policy_start_date")).alias("second_policy_start_date_week"),
    )
    .orderBy(
        F.col("policy_status").asc_nulls_last(),
        F.col("last_claim_date").asc(),
        "id",
    )
    .limit(10)
)
