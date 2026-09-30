from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id")
w_all_by_claimed_amount = Window.orderBy(F.col("claimed_amount").desc(), "id")
w_policy_id = Window.partitionBy("policy_id")
w_all_by_policy_premium = Window.orderBy(F.col("policy_premium").desc(), "id")
w_all_by_adjuster_level_ties = Window.orderBy(F.col("adjuster_level").asc_nulls_last())
w_all_by_adjuster_id_min_polic = Window.orderBy("adjuster_id_min_policy_premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_adjuster_level_full = Window.orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_level = Window.partitionBy("adjuster_level")
w_reported_at_month_approved_a = Window.partitionBy(F.date_trunc("month", "reported_at"), "approved_amount_tier").orderBy(F.col("reported_at").desc(), "id")
w_all_by_adjuster_level_ties_2 = Window.orderBy(F.col("adjuster_level").desc())
w_reported_at_month_approved_a_2 = Window.partitionBy(F.date_trunc("month", "reported_at"), "approved_amount_tier").orderBy(F.col("reported_at").desc())
w_all_by_adjuster_level_prior = Window.orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(-6, -1)

valid_claims = (
    df_claims
    .filter(F.col("status") == "REJECTED")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        F.when(F.col("approved_amount") >= 14377.6, "A").when(F.col("approved_amount") >= 0.0, "B").otherwise("C").alias("approved_amount_tier"),
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
    )
)

with_details_adjuster = (
    valid_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.reported_at",
        "s.approved_amount_tier",
        "s.approved_amount_plus_claimed_amount",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("p.product").alias("policy_product"),
        F.col("p.premium").alias("policy_premium"),
    )
)

windowed = (
    with_details_adjuster
    .select(
        "*",
        F.min("policy_premium").over(w_adjuster_id).alias("adjuster_id_min_policy_premium"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_reported_at)).alias("days_since_prev_claim_date"),
        F.rank().over(w_all_by_claim_date).alias("claim_date_rank"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_adjuster_id), 2).alias("adjuster_level_vs_adjuster_id_avg"),
        F.round(F.percent_rank().over(w_all_by_claimed_amount), 3).alias("claimed_amount_percent_rank"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.max("claimed_amount").over(w_policy_id).alias("policy_id_max_claimed_amount"),
        F.lag("adjuster_level").over(w_all_by_claim_date).alias("prev_adjuster_level"),
        F.row_number().over(w_all_by_claim_date).alias("seq_no"),
        F.lead("reported_at").over(w_all_by_claimed_amount).alias("next_reported_at"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_policy_premium)).alias("days_since_prev_claim_date_2"),
        (F.col("adjuster_level") - F.lag("adjuster_level").over(w_all_by_policy_premium)).alias("adjuster_level_change"),
        F.round(F.percent_rank().over(w_all_by_adjuster_level_ties), 3).alias("adjuster_level_percent_rank"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount_tier",
        "approved_amount_plus_claimed_amount",
        "adjuster_team",
        "policy_product",
        "policy_premium",
        "adjuster_id_min_policy_premium",
        "days_since_prev_claim_date",
        "claim_date_rank",
        "adjuster_level_vs_adjuster_id_avg",
        "claimed_amount_percent_rank",
        "policy_id_max_claimed_amount",
        "prev_adjuster_level",
        "seq_no",
        "next_reported_at",
        "adjuster_level_change",
        F.ntile(4).over(w_all_by_policy_premium).alias("policy_premium_quartile"),
        F.count(F.lit(1)).over(w_all_by_adjuster_id_min_polic).alias("running_count"),
        F.nth_value("next_reported_at", 2).over(w_all_by_adjuster_level_full).alias("second_next_reported_at"),
        F.sum("prev_adjuster_level").over(w_adjuster_level).alias("adjuster_level_sum_prev_adjuster_level"),
        F.lag("prev_adjuster_level").over(w_reported_at_month_approved_a).alias("prev_prev_adjuster_level"),
        F.rank().over(w_all_by_adjuster_level_ties_2).alias("adjuster_level_rank"),
        F.dense_rank().over(w_reported_at_month_approved_a_2).alias("reported_at_dense_rank"),
        F.round(F.avg("adjuster_level_change").over(w_all_by_adjuster_level_prior), 2).alias("adjuster_level_change_avg_6_prior"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "approved_amount_plus_claimed_amount",
        "adjuster_team",
        "policy_product",
        "policy_premium",
        "days_since_prev_claim_date",
        "claim_date_rank",
        "adjuster_level_vs_adjuster_id_avg",
        "claimed_amount_percent_rank",
        "prev_adjuster_level",
        "seq_no",
        "next_reported_at",
        "policy_premium_quartile",
        "second_next_reported_at",
        "prev_prev_adjuster_level",
        "adjuster_level_rank",
        "reported_at_dense_rank",
        "adjuster_level_change_avg_6_prior",
    )
    .orderBy(F.col("second_next_reported_at").asc())
)
