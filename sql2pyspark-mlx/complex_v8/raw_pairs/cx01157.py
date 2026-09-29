from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_adjuster_id_by_rep = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("reported_at").desc(), "id")
w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_claim_date_year_adjuster_id = Window.partitionBy(F.year("claim_date"), "adjuster_id").orderBy(F.col("claim_date").desc(), "id")
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_claim_date_year_adjuster_id_2 = Window.partitionBy(F.year("claim_date"), "adjuster_id").orderBy(F.col("claim_date").desc())
w_policy_id_adjuster_id_by_rep_2 = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_claim_date_month = Window.partitionBy("adjuster_id", F.date_trunc("month", "claim_date")).orderBy("reported_at")
w_all_by_reported_at_full = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_month_by_claim_d = Window.partitionBy(F.date_trunc("month", "reported_at")).orderBy("claim_date")
w_adjuster_id_claim_date_month_2 = Window.partitionBy("adjuster_id", F.date_trunc("month", "claim_date")).orderBy("reported_at", "id")
w_adjuster_id_claim_date_month_3 = Window.partitionBy("adjuster_id", F.date_trunc("month", "claim_date")).orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_last_known_claim_date = Window.orderBy(F.col("last_known_claim_date").desc(), "id")
w_all_by_prev3_reported_at_tie = Window.orderBy("prev3_reported_at")
w_all_by_prev3_reported_at = Window.orderBy("prev3_reported_at", "id")
w_first_reported_at_year = Window.partitionBy(F.year("first_reported_at"))
w_all_by_prev3_reported_at_ful = Window.orderBy("prev3_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_claims = (
    df_claims
    .filter(F.col("adjuster_id") <= 7)
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
    valid_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        F.lag("reported_at", 3).over(w_policy_id_adjuster_id_by_rep).alias("prev3_reported_at"),
        F.dense_rank().over(w_all_by_reported_at_ties).alias("reported_at_dense_rank"),
        F.lag("claim_date", 2).over(w_claim_date_year_adjuster_id).alias("prev2_claim_date"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_all_by_reported_at))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_claim_date_year_adjuster_id)).alias("days_since_prev_claim_date"),
        F.lead("reported_at", 3).over(w_all_by_reported_at).alias("next3_reported_at"),
        F.round(F.percent_rank().over(w_claim_date_year_adjuster_id_2), 3).alias("claim_date_percent_rank"),
        F.first("claim_date").over(w_policy_id_adjuster_id_by_rep_2).alias("first_claim_date"),
        F.round(F.percent_rank().over(w_adjuster_id_claim_date_month), 3).alias("reported_at_percent_rank"),
        F.lead("claim_date").over(w_policy_id_adjuster_id_by_rep).alias("next_claim_date"),
        F.last("claim_date").over(w_all_by_reported_at_full).alias("last_claim_date"),
        F.round(F.cume_dist().over(w_reported_at_month_by_claim_d), 3).alias("claim_date_cume_dist"),
        F.first("reported_at").over(w_all_by_reported_at_full).alias("first_reported_at"),
        F.row_number().over(w_adjuster_id_claim_date_month_2).alias("rn"),
        F.dense_rank().over(w_claim_date_year_adjuster_id_2).alias("claim_date_dense_rank"),
        F.last("claim_date", ignorenulls=True).over(w_adjuster_id_claim_date_month_3).alias("last_known_claim_date"),
    )
)

ranked = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        "reported_at_dense_rank",
        "prev2_claim_date",
        "minutes_since_prev_reported_at",
        "days_since_prev_claim_date",
        "next3_reported_at",
        "claim_date_percent_rank",
        "first_claim_date",
        "reported_at_percent_rank",
        "next_claim_date",
        "last_claim_date",
        "claim_date_cume_dist",
        "first_reported_at",
        "rn",
        "last_known_claim_date",
        F.ntile(3).over(w_all_by_last_known_claim_date).alias("last_known_claim_date_tercile"),
        F.round(F.cume_dist().over(w_all_by_prev3_reported_at_tie), 3).alias("prev3_reported_at_cume_dist"),
        F.ntile(10).over(w_all_by_prev3_reported_at).alias("prev3_reported_at_decile"),
        F.round(F.col("claim_date_dense_rank") / F.nullif(F.sum("claim_date_dense_rank").over(w_first_reported_at_year), F.lit(0)), 4).alias("claim_date_dense_rank_share_of_first_reported_at"),
        F.size(F.collect_set("policy_id").over(w_first_reported_at_year)).alias("distinct_policy_id_in_first_reported_at_year"),
        F.first("claim_date_dense_rank").over(w_all_by_prev3_reported_at_ful).alias("first_claim_date_dense_rank"),
        F.count(F.lit(1)).over(w_first_reported_at_year).alias("rows_in_first_reported_at_year"),
        F.rank().over(w_all_by_last_known_claim_date).alias("last_known_claim_date_rank"),
    )
)

result = (
    ranked
    .filter(F.col("rows_in_first_reported_at_year") > 4)
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at_dense_rank",
        "prev2_claim_date",
        "minutes_since_prev_reported_at",
        "days_since_prev_claim_date",
        "next3_reported_at",
        "claim_date_percent_rank",
        "first_claim_date",
        "first_reported_at",
        "rn",
        "last_known_claim_date",
        "last_known_claim_date_tercile",
        "prev3_reported_at_decile",
        "distinct_policy_id_in_first_reported_at_year",
        "rows_in_first_reported_at_year",
        "last_known_claim_date_rank",
        F.round(F.col("reported_at_percent_rank") * 0.25, 2).alias("reported_at_percent_rank_adj"),
    )
    .orderBy("adjuster_id", "id")
    .limit(5)
)
