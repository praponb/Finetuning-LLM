from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_level_by_claimed_am = Window.partitionBy("adjuster_level").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(-2, Window.currentRow)
w_adjuster_level_by_claimed_am_2 = Window.partitionBy("adjuster_level").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_adjuster_level = Window.orderBy(F.col("adjuster_level").asc_nulls_last(), "id")
w_all_by_adjuster_level_ties = Window.orderBy(F.col("adjuster_level").desc())
w_reported_at_month_by_reporte = Window.partitionBy(F.date_trunc("month", "reported_at")).orderBy("reported_at", "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_policy_id = Window.partitionBy("policy_id")
w_all_by_adjuster_level_runnin = Window.orderBy(F.col("adjuster_level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_adjuster_level = Window.partitionBy("second_adjuster_level")
w_adjuster_level_rank_adjuster = Window.partitionBy("adjuster_level_rank", "adjuster_team").orderBy("prev_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_adjuster_level_by_pre = Window.partitionBy("second_adjuster_level").orderBy("prev_reported_at", "id")
w_second_adjuster_level_by_pre_2 = Window.partitionBy("second_adjuster_level").orderBy("prev_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_level_rank_adjuster_2 = Window.partitionBy("adjuster_level_rank", "adjuster_team").orderBy("prev_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_level_rank_adjuster_3 = Window.partitionBy("adjuster_level_rank", "adjuster_team").orderBy("prev_reported_at", "id")

base_claims = (
    df_claims
    .filter(F.col("status") == "IN_REVIEW")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

with_parent = (
    base_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

ranked = (
    with_parent
    .select(
        "*",
        F.round(F.avg("adjuster_level").over(w_adjuster_level_by_claimed_am), 2).alias("adjuster_level_avg_3"),
        F.nth_value("adjuster_level", 2).over(w_adjuster_level_by_claimed_am_2).alias("second_adjuster_level"),
        F.lag("claimed_amount", 3).over(w_all_by_adjuster_level).alias("prev3_claimed_amount"),
        F.rank().over(w_all_by_adjuster_level_ties).alias("adjuster_level_rank"),
        F.lead("adjuster_level", 3).over(w_reported_at_month_by_reporte).alias("next3_adjuster_level"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_id), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_id"),
        F.lag("reported_at").over(w_reported_at_month_by_reporte).alias("prev_reported_at"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_id), 2).alias("claimed_amount_vs_policy_id_avg"),
        F.sum("claimed_amount").over(w_all_by_adjuster_level_runnin).alias("running_claimed_amount"),
        F.min("adjuster_level").over(w_policy_id).alias("policy_id_min_adjuster_level"),
        F.round(F.cume_dist().over(w_all_by_adjuster_level), 3).alias("adjuster_level_cume_dist"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.round(F.col("adjuster_level_avg_3") / F.nullif(F.sum("adjuster_level_avg_3").over(w_second_adjuster_level), F.lit(0)), 4).alias("adjuster_level_avg_3_share_of_second_adjuster_le"),
        F.nth_value("adjuster_level_cume_dist", 2).over(w_adjuster_level_rank_adjuster).alias("second_adjuster_level_cume_dist"),
        F.lead("reported_at", 3).over(w_second_adjuster_level_by_pre).alias("next3_reported_at"),
        F.round(F.avg("next3_adjuster_level").over(w_second_adjuster_level), 2).alias("second_adjuster_level_avg_next3_adjuster_level"),
        F.nth_value("claimed_amount", 2).over(w_second_adjuster_level_by_pre_2).alias("second_claimed_amount"),
        F.last("claimed_amount", ignorenulls=True).over(w_adjuster_level_rank_adjuster_2).alias("last_known_claimed_amount"),
        F.round(F.cume_dist().over(w_adjuster_level_rank_adjuster_3), 3).alias("prev_reported_at_cume_dist"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "reported_at",
        "adjuster_level",
        "adjuster_team",
        "adjuster_level_avg_3",
        "second_adjuster_level",
        "adjuster_level_rank",
        "next3_adjuster_level",
        "claimed_amount_share_of_adjuster_id",
        "running_claimed_amount",
        "policy_id_min_adjuster_level",
        "adjuster_level_cume_dist",
        "adjuster_level_avg_3_share_of_second_adjuster_le",
        "second_adjuster_level_cume_dist",
        "next3_reported_at",
        "last_known_claimed_amount",
        "prev_reported_at_cume_dist",
        F.length(F.trim(F.col("adjuster_team"))).alias("adjuster_team_len"),
        F.quarter(F.col("prev_reported_at")).alias("prev_reported_at_quarter"),
    )
    .orderBy(F.col("second_adjuster_level_cume_dist").desc(), "id")
    .limit(10)
)
