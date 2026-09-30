from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_team = Window.partitionBy("adjuster_team")
w_adjuster_level_by_approved_a = Window.partitionBy("adjuster_level").orderBy("approved_amount_adj", "id")
w_claim_type_claim_date_month = Window.partitionBy("claim_type", F.month("claim_date")).orderBy(F.col("approved_amount_adj").desc(), "id")
w_adjuster_id_by_claim_date_ti = Window.partitionBy("adjuster_id").orderBy("claim_date")
w_adjuster_id_by_claim_date_fu = Window.partitionBy("adjuster_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_by_claim_date_mo = Window.partitionBy("adjuster_id").orderBy("claim_date", "id").rowsBetween(-3, Window.currentRow)
w_adjuster_level_second_claim = Window.partitionBy("adjuster_level", F.month("second_claim_date"))
w_distinct_adjuster_id_in_adju = Window.partitionBy("distinct_adjuster_id_in_adjuster_team", "policy_id").orderBy(F.col("adjuster_team_avg_adjuster_level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_claim_date_month_poli = Window.partitionBy(F.month("second_claim_date"), "policy_id").orderBy(F.col("claim_date_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_claim_date_month_poli_2 = Window.partitionBy(F.month("second_claim_date"), "policy_id").orderBy(F.col("claim_date_cume_dist").desc())
w_distinct_adjuster_id_in_adju_2 = Window.partitionBy("distinct_adjuster_id_in_adjuster_team", "policy_id").orderBy(F.col("adjuster_team_avg_adjuster_level").desc(), "id")

valid = (
    df_claims
    .filter((F.col("claim_type") == "flood") & (F.col("approved_amount") >= 0.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        F.round(F.col("approved_amount") * 0.9, 2).alias("approved_amount_adj"),
    )
)

enriched_adjuster = (
    valid.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount_adj",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

with_windows = (
    enriched_adjuster
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount_adj",
        "adjuster_name",
        "adjuster_level",
        F.round(F.avg("adjuster_level").over(w_adjuster_team), 2).alias("adjuster_team_avg_adjuster_level"),
        F.row_number().over(w_adjuster_level_by_approved_a).alias("approved_amount_adj_seq"),
        F.row_number().over(w_claim_type_claim_date_month).alias("rn"),
        F.size(F.collect_set("adjuster_id").over(w_adjuster_team)).alias("distinct_adjuster_id_in_adjuster_team"),
        F.lag("claim_date").over(w_claim_type_claim_date_month).alias("prev_claim_date"),
        F.min("adjuster_level").over(w_adjuster_team).alias("adjuster_team_min_adjuster_level"),
        F.sum("approved_amount_adj").over(w_adjuster_team).alias("adjuster_team_sum_approved_amount_adj"),
        F.round(F.cume_dist().over(w_adjuster_id_by_claim_date_ti), 3).alias("claim_date_cume_dist"),
        F.nth_value("claim_date", 2).over(w_adjuster_id_by_claim_date_fu).alias("second_claim_date"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_adjuster_team), 2).alias("adjuster_level_vs_adjuster_team_avg"),
        F.sum("approved_amount_adj").over(w_adjuster_id_by_claim_date_mo).alias("approved_amount_adj_sum_4"),
        F.round(F.avg("approved_amount_adj").over(w_adjuster_team), 2).alias("adjuster_team_avg_approved_amount_adj"),
        F.max("approved_amount_adj").over(w_adjuster_team).alias("adjuster_team_max_approved_amount_adj"),
    )
)

with_trends = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "adjuster_level",
        "adjuster_team_avg_adjuster_level",
        "approved_amount_adj_seq",
        "rn",
        "prev_claim_date",
        "adjuster_team_min_adjuster_level",
        "adjuster_team_sum_approved_amount_adj",
        "claim_date_cume_dist",
        "second_claim_date",
        "adjuster_level_vs_adjuster_team_avg",
        "approved_amount_adj_sum_4",
        "adjuster_team_avg_approved_amount_adj",
        "adjuster_team_max_approved_amount_adj",
        F.round(F.col("approved_amount_adj_sum_4") / F.nullif(F.sum("approved_amount_adj_sum_4").over(w_adjuster_level_second_claim), F.lit(0)), 4).alias("approved_amount_adj_sum_4_share_of_adjuster_leve"),
        F.last("approved_amount_adj").over(w_distinct_adjuster_id_in_adju).alias("last_approved_amount_adj"),
        F.last("approved_amount_adj_sum_4").over(w_second_claim_date_month_poli).alias("last_approved_amount_adj_sum_4"),
        F.round(F.percent_rank().over(w_second_claim_date_month_poli_2), 3).alias("claim_date_cume_dist_percent_rank"),
        F.round(F.cume_dist().over(w_distinct_adjuster_id_in_adju_2), 3).alias("adjuster_team_avg_adjuster_level_cume_dist"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_adjuster_level_second_claim), 2).alias("adjuster_level_vs_adjuster_level_second_claim_da"),
        F.row_number().over(w_distinct_adjuster_id_in_adju_2).alias("adjuster_team_avg_adjuster_level_seq"),
        F.sum("approved_amount_adj_seq").over(w_adjuster_level_second_claim).alias("adjuster_level_second_claim_date_month_sum_appro"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "policy_id",
        "adjuster_level",
        "adjuster_team_avg_adjuster_level",
        "approved_amount_adj_seq",
        "rn",
        "prev_claim_date",
        "adjuster_team_min_adjuster_level",
        "second_claim_date",
        "adjuster_level_vs_adjuster_team_avg",
        "approved_amount_adj_sum_4",
        "adjuster_team_avg_approved_amount_adj",
        "adjuster_team_max_approved_amount_adj",
        "last_approved_amount_adj_sum_4",
        "claim_date_cume_dist_percent_rank",
        "adjuster_team_avg_adjuster_level_cume_dist",
        "adjuster_level_vs_adjuster_level_second_claim_da",
        "adjuster_team_avg_adjuster_level_seq",
        "adjuster_level_second_claim_date_month_sum_appro",
    )
    .orderBy("id")
)
