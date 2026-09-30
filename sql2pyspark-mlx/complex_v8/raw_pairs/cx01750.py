from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_policy_status_policy_id = Window.partitionBy("policy_status", "policy_id")
w_all_by_policy_start_date_mov = Window.orderBy("policy_start_date", "id").rowsBetween(-1, Window.currentRow)
w_all_by_policy_start_date_ful = Window.orderBy("policy_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claimed_amount_ties = Window.orderBy("claimed_amount")
w_all_by_policy_start_date_pri = Window.orderBy("policy_start_date", "id").rowsBetween(-2, -1)
w_policy_status = Window.partitionBy("policy_status")
w_policy_start_date_month_by_c = Window.partitionBy(F.date_trunc("month", "policy_start_date")).orderBy(F.col("claimed_amount_max_2_prior").desc_nulls_first(), "id")
w_policy_status_by_first_polic = Window.partitionBy("policy_status").orderBy(F.col("first_policy_start_date").desc_nulls_first(), "id")
w_policy_status_by_first_polic_2 = Window.partitionBy("policy_status").orderBy(F.col("first_policy_start_date").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_claims
    .filter((F.col("adjuster_id") <= 2) | (F.col("claimed_amount") >= 18924.4))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        F.date_trunc("month", F.col("reported_at")).alias("reported_at_month"),
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
    )
)

with_details = (
    filtered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.reported_at",
        "s.claim_date_week",
        F.col("p.status").alias("policy_status"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

ranked = (
    with_details
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_claimed_amount), 3).alias("claimed_amount_cume_dist"),
        F.lead("reported_at").over(w_all_by_claimed_amount).alias("next_reported_at"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_status_policy_id), 2).alias("claimed_amount_vs_policy_status_policy_id_avg"),
        F.count(F.lit(1)).over(w_policy_status_policy_id).alias("rows_in_policy_status_policy_id"),
        F.sum("claimed_amount").over(w_all_by_policy_start_date_mov).alias("claimed_amount_sum_2"),
        F.first("policy_start_date").over(w_all_by_policy_start_date_ful).alias("first_policy_start_date"),
        F.round(F.percent_rank().over(w_all_by_claimed_amount_ties), 3).alias("claimed_amount_percent_rank"),
        F.max("claimed_amount").over(w_all_by_policy_start_date_pri).alias("claimed_amount_max_2_prior"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "reported_at",
        "policy_start_date",
        "policy_holder_id",
        "claimed_amount_cume_dist",
        "next_reported_at",
        "claimed_amount_vs_policy_status_policy_id_avg",
        "rows_in_policy_status_policy_id",
        "claimed_amount_sum_2",
        "first_policy_start_date",
        "claimed_amount_max_2_prior",
        F.count(F.lit(1)).over(w_policy_status).alias("rows_in_policy_status"),
        F.lead("reported_at", 3).over(w_policy_start_date_month_by_c).alias("next3_reported_at"),
        F.lag("reported_at", 3).over(w_policy_status_by_first_polic).alias("prev3_reported_at"),
        F.round(F.avg("claimed_amount_percent_rank").over(w_policy_status_by_first_polic_2), 2).alias("claimed_amount_percent_rank_avg_7"),
        F.ntile(10).over(w_policy_status_by_first_polic).alias("first_policy_start_date_decile"),
    )
)

result = (
    windowed
    .filter(F.col("claimed_amount_vs_policy_status_policy_id_avg") <= 10000.0)
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "reported_at",
        "policy_start_date",
        "policy_holder_id",
        "claimed_amount_cume_dist",
        "next_reported_at",
        "claimed_amount_vs_policy_status_policy_id_avg",
        "rows_in_policy_status_policy_id",
        "claimed_amount_sum_2",
        "first_policy_start_date",
        "claimed_amount_max_2_prior",
        "rows_in_policy_status",
        "prev3_reported_at",
        "first_policy_start_date_decile",
    )
    .orderBy(F.col("policy_start_date").desc())
)
