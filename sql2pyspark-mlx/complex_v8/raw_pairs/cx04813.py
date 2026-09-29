from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_team_policy_start_d = Window.partitionBy("adjuster_team", F.date_trunc("month", "policy_start_date"))
w_adjuster_team_by_reported_at = Window.partitionBy("adjuster_team").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_team_by_reported_at_2 = Window.partitionBy("adjuster_team").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claimed_amount_moving = Window.orderBy("claimed_amount", "id").rowsBetween(-3, Window.currentRow)
w_all_by_claimed_amount_ties = Window.orderBy("claimed_amount")
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_team_by_reported_at_3 = Window.partitionBy("adjuster_team").orderBy("reported_at", "id").rowsBetween(-3, -1)
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_adjuster_team_by_reported_at_4 = Window.partitionBy("adjuster_team").orderBy("reported_at", "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_claim_date_month_by_adjuster = Window.partitionBy(F.month("claim_date")).orderBy("adjuster_level_share_of_adjuster_team_policy_sta", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_reported_at_running_2 = Window.orderBy(F.col("reported_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_claims = (
    df_claims
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

with_parent_adjuster = (
    scoped_claims.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.upper(F.trim(F.col("a.adjuster_name"))).like("E%")), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

windowed = (
    with_parent_adjuster
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "reported_at",
        "adjuster_team",
        "adjuster_level",
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_adjuster_team_policy_start_d), F.lit(0)), 4).alias("adjuster_level_share_of_adjuster_team_policy_sta"),
        F.first("claimed_amount", ignorenulls=True).over(w_adjuster_team_by_reported_at).alias("first_known_claimed_amount"),
        F.nth_value("adjuster_level", 2).over(w_adjuster_team_by_reported_at_2).alias("second_adjuster_level"),
        F.round(F.avg("adjuster_level").over(w_all_by_claimed_amount_moving), 2).alias("adjuster_level_avg_4"),
        F.round(F.cume_dist().over(w_all_by_claimed_amount_ties), 3).alias("claimed_amount_cume_dist"),
        F.first("policy_start_date", ignorenulls=True).over(w_all_by_reported_at_running).alias("first_known_policy_start_date"),
        F.last("adjuster_level", ignorenulls=True).over(w_all_by_reported_at_running).alias("last_known_adjuster_level"),
        F.max("adjuster_level").over(w_adjuster_team_by_reported_at_3).alias("adjuster_level_max_3_prior"),
        F.round(F.avg("claimed_amount").over(w_adjuster_team_policy_start_d), 2).alias("adjuster_team_policy_start_date_month_avg_claime"),
        F.lead("claim_date", 2).over(w_all_by_claimed_amount).alias("next2_claim_date"),
        F.ntile(3).over(w_adjuster_team_by_reported_at_4).alias("reported_at_tercile"),
    )
)

tagged = (
    windowed
    .filter(F.col("second_adjuster_level") <= 3)
    .select(
        "*",
        F.round(F.col("adjuster_level_avg_4") * 1.1, 2).alias("adjuster_level_avg_4_adj"),
        F.substring(F.trim(F.col("adjuster_team")), 1, 4).alias("adjuster_team_prefix"),
    )
)

ranked = (
    tagged
    .select(
        "id",
        "adjuster_id",
        "reported_at",
        "adjuster_team",
        "adjuster_level",
        "adjuster_level_share_of_adjuster_team_policy_sta",
        "first_known_claimed_amount",
        "second_adjuster_level",
        "adjuster_level_avg_4",
        "last_known_adjuster_level",
        "adjuster_level_max_3_prior",
        "next2_claim_date",
        "adjuster_level_avg_4_adj",
        F.round(F.col("adjuster_level_avg_4") / F.nullif(F.sum("adjuster_level_avg_4").over(w_adjuster_id), F.lit(0)), 4).alias("adjuster_level_avg_4_share_of_adjuster_id"),
        F.round(F.col("first_known_claimed_amount") - F.avg("first_known_claimed_amount").over(w_adjuster_id), 2).alias("first_known_claimed_amount_vs_adjuster_id_avg"),
        F.first("next2_claim_date").over(w_claim_date_month_by_adjuster).alias("first_next2_claim_date"),
        F.size(F.collect_set("adjuster_team").over(w_adjuster_id)).alias("distinct_adjuster_team_in_adjuster_id"),
        F.first("claim_date", ignorenulls=True).over(w_all_by_reported_at_running_2).alias("first_known_claim_date"),
        F.round(F.avg("last_known_adjuster_level").over(w_adjuster_id), 2).alias("adjuster_id_avg_last_known_adjuster_level"),
        F.nth_value("second_adjuster_level", 2).over(w_claim_date_month_by_adjuster).alias("second_second_adjuster_level"),
    )
)

result = (
    ranked
    .filter(F.col("adjuster_level_max_3_prior") < 12)
    .select(
        "id",
        "reported_at",
        "adjuster_level",
        "adjuster_level_share_of_adjuster_team_policy_sta",
        "second_adjuster_level",
        "adjuster_level_avg_4",
        "last_known_adjuster_level",
        "next2_claim_date",
        "adjuster_level_avg_4_adj",
        "adjuster_level_avg_4_share_of_adjuster_id",
        "first_next2_claim_date",
        "distinct_adjuster_team_in_adjuster_id",
        "first_known_claim_date",
        "adjuster_id_avg_last_known_adjuster_level",
        "second_second_adjuster_level",
    )
)
