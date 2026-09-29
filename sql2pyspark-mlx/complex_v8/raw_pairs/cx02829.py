from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_team_status = Window.partitionBy("adjuster_team", "status")
w_adjuster_team_by_claimed_amo = Window.partitionBy("adjuster_team").orderBy("claimed_amount_adj")
w_adjuster_level_by_claimed_am = Window.partitionBy("adjuster_level").orderBy("claimed_amount_adj", "id")
w_status_by_claimed_amount = Window.partitionBy("status").orderBy(F.col("claimed_amount").desc(), "id")
w_adjuster_level_adjuster_team = Window.partitionBy("adjuster_level", "adjuster_team").orderBy("adjuster_level", "id")
w_reported_at_month_reported_a = Window.partitionBy(F.month("reported_at"), F.to_date("reported_at_day"))
w_next_reported_at_year_to_dat = Window.partitionBy(F.to_date("next_reported_at_year"), "adjuster_level").orderBy("claimed_amount_adj_change", "id")
w_all_by_next_reported_at = Window.orderBy(F.col("next_reported_at").desc(), "id")
w_all_by_next_reported_at_runn = Window.orderBy(F.col("next_reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.round(F.col("claimed_amount") * 1.1, 2).alias("claimed_amount_adj"),
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
    )
)

with_details = (
    prepared_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.level") > 1)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claimed_amount_adj",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

metrics = (
    with_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
        "adjuster_level",
        F.count(F.lit(1)).over(w_adjuster_team_status).alias("rows_in_adjuster_team_status"),
        F.round(F.percent_rank().over(w_adjuster_team_by_claimed_amo), 3).alias("claimed_amount_adj_percent_rank"),
        F.lead("reported_at").over(w_adjuster_level_by_claimed_am).alias("next_reported_at"),
        F.round(F.cume_dist().over(w_status_by_claimed_amount), 3).alias("claimed_amount_cume_dist"),
        (F.col("claimed_amount_adj") - F.lag("claimed_amount_adj").over(w_adjuster_level_adjuster_team)).alias("claimed_amount_adj_change"),
        F.max("adjuster_level").over(w_adjuster_team_status).alias("adjuster_team_status_max_adjuster_level"),
        F.row_number().over(w_status_by_claimed_amount).alias("rn"),
        F.round(F.cume_dist().over(w_adjuster_level_by_claimed_am), 3).alias("claimed_amount_adj_cume_dist"),
        F.min("approved_amount").over(w_adjuster_team_status).alias("adjuster_team_status_min_approved_amount"),
    )
)

derived = (
    metrics
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.date_trunc("day", F.col("reported_at")).alias("reported_at_day"),
        F.date_trunc("year", F.col("next_reported_at")).alias("next_reported_at_year"),
    )
)

sequenced = (
    derived
    .select(
        "*",
        F.sum("rn").over(w_reported_at_month_reported_a).alias("reported_at_month_reported_at_day_to_date_sum_rn"),
        F.lag("adjuster_level").over(w_next_reported_at_year_to_dat).alias("prev_adjuster_level"),
        F.ntile(5).over(w_next_reported_at_year_to_dat).alias("claimed_amount_adj_change_quintile"),
        F.lag("rows_in_adjuster_team_status").over(w_all_by_next_reported_at).alias("prev_rows_in_adjuster_team_status"),
        F.sum("rows_in_adjuster_team_status").over(w_all_by_next_reported_at_runn).alias("running_rows_in_adjuster_team_status"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "adjuster_level",
        "rows_in_adjuster_team_status",
        "claimed_amount_adj_percent_rank",
        "next_reported_at",
        "claimed_amount_cume_dist",
        "claimed_amount_adj_change",
        "rn",
        "claimed_amount_adj_cume_dist",
        "adjuster_team_status_min_approved_amount",
        "reported_at_day",
        "reported_at_month_reported_at_day_to_date_sum_rn",
        "claimed_amount_adj_change_quintile",
        "prev_rows_in_adjuster_team_status",
        "running_rows_in_adjuster_team_status",
        F.when(F.col("status").isin("REJECTED", "APPROVED"), "group_a").when(F.col("status") == "OPEN", "group_b").otherwise("other").alias("status_group"),
    )
)
