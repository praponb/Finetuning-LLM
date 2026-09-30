from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_claim_date_month_adjuster_id = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_id").orderBy(F.col("claim_date").asc_nulls_last())
w_claim_date_month_adjuster_id_2 = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_id").orderBy(F.col("claim_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_team_by_claim_date = Window.partitionBy("adjuster_team").orderBy(F.col("claim_date").desc(), "id")
w_adjuster_team_adjuster_id_by = Window.partitionBy("adjuster_team", "adjuster_id").orderBy("claim_date", "id")
w_adjuster_team_policy_id = Window.partitionBy("adjuster_team", "policy_id")
w_claim_date_dense_rank = Window.partitionBy("claim_date_dense_rank")
w_status_claim_date_year_by_pr = Window.partitionBy("status", F.year("claim_date")).orderBy("prev_claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_rn = Window.orderBy("rn", "id")
w_status_claim_date_year_by_pr_2 = Window.partitionBy("status", F.year("claim_date")).orderBy("prev_claim_date", "id")
w_status_claim_date_year_by_pr_3 = Window.partitionBy("status", F.year("claim_date")).orderBy("prev_claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_claims = df_claims

with_details = (
    prepared_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        F.col("a.team").alias("adjuster_team"),
    )
)

metrics = (
    with_details
    .select(
        "*",
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_all_by_claim_date)).alias("claimed_amount_change"),
        F.dense_rank().over(w_claim_date_month_adjuster_id).alias("claim_date_dense_rank"),
        F.max("claimed_amount").over(w_claim_date_month_adjuster_id_2).alias("max_claimed_amount_so_far"),
        F.row_number().over(w_adjuster_team_by_claim_date).alias("rn"),
        F.lead("claimed_amount", 1, 0).over(w_adjuster_team_by_claim_date).alias("next_claimed_amount"),
        F.size(F.collect_set("claim_type").over(w_adjuster_id)).alias("distinct_claim_type_in_adjuster_id"),
        F.lag("claim_date").over(w_adjuster_team_adjuster_id_by).alias("prev_claim_date"),
        F.count(F.lit(1)).over(w_adjuster_team_policy_id).alias("rows_in_adjuster_team_policy_id"),
    )
)

leaders = metrics.filter(F.col("rn") == 1)

with_windows = (
    leaders
    .select(
        "*",
        F.round(F.col("max_claimed_amount_so_far") / F.nullif(F.sum("max_claimed_amount_so_far").over(w_claim_date_dense_rank), F.lit(0)), 4).alias("max_claimed_amount_so_far_share_of_claim_date_de"),
        F.nth_value("rn", 2).over(w_status_claim_date_year_by_pr).alias("second_rn"),
        F.lead("claim_date").over(w_all_by_rn).alias("next_claim_date"),
        F.lead("claim_date_dense_rank").over(w_status_claim_date_year_by_pr_2).alias("next_claim_date_dense_rank"),
        F.count(F.lit(1)).over(w_claim_date_dense_rank).alias("rows_in_claim_date_dense_rank"),
        F.lead("rn", 3).over(w_status_claim_date_year_by_pr_2).alias("next3_rn"),
        F.last("rn", ignorenulls=True).over(w_status_claim_date_year_by_pr_3).alias("last_known_rn"),
        F.ntile(5).over(w_status_claim_date_year_by_pr_2).alias("prev_claim_date_quintile"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "adjuster_team",
        "rows_in_adjuster_id",
        "claimed_amount_change",
        "claim_date_dense_rank",
        "max_claimed_amount_so_far",
        "next_claimed_amount",
        "distinct_claim_type_in_adjuster_id",
        "prev_claim_date",
        "rows_in_adjuster_team_policy_id",
        "max_claimed_amount_so_far_share_of_claim_date_de",
        "next_claim_date_dense_rank",
        "next3_rn",
        "last_known_rn",
    )
    .orderBy("next3_rn")
)
