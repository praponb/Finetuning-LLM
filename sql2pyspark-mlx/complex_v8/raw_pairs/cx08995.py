from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_team_policy_id_by_c = Window.partitionBy("adjuster_team", "policy_id").orderBy(F.col("claim_date").desc(), "id")
w_adjuster_level_adjuster_id_b = Window.partitionBy("adjuster_level", "adjuster_id").orderBy("adjuster_level", "id").rowsBetween(-6, Window.currentRow)
w_policy_id_adjuster_team = Window.partitionBy("policy_id", "adjuster_team")
w_policy_id = Window.partitionBy("policy_id")
w_claim_date_month_by_claim_da = Window.partitionBy(F.month("claim_date")).orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_month_by_claim_da_2 = Window.partitionBy(F.month("claim_date")).orderBy(F.col("claim_date").desc(), "id")
w_adjuster_id_by_claim_date = Window.partitionBy("adjuster_id").orderBy("claim_date", "id")
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").desc())
w_adjuster_id_claim_date_month = Window.partitionBy("adjuster_id", F.date_trunc("month", "claim_date"))
w_adjuster_id_claim_date_month_2 = Window.partitionBy("adjuster_id", "claim_date_month").orderBy(F.col("policy_id_adjuster_team_max_adjuster_level").desc(), "id")
w_adjuster_id_claim_date_month_3 = Window.partitionBy("adjuster_id", "claim_date_month").orderBy(F.col("last_adjuster_level").desc(), "id")
w_adjuster_id_claim_date_month_4 = Window.partitionBy("adjuster_id", "claim_date_month").orderBy(F.col("policy_id_adjuster_team_max_adjuster_level").desc(), "id").rowsBetween(-3, -1)

cleaned_claims = (
    df_claims
    .filter(F.col("claim_type").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.abs(F.col("approved_amount") - 9608.4).alias("approved_amount_distance"),
    )
)

combined_details = (
    cleaned_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

with_windows = (
    combined_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "adjuster_level",
        "adjuster_name",
        F.lag("adjuster_level").over(w_adjuster_team_policy_id_by_c).alias("prev_adjuster_level"),
        F.round(F.avg("adjuster_level").over(w_adjuster_level_adjuster_id_b), 2).alias("adjuster_level_avg_7"),
        F.max("adjuster_level").over(w_policy_id_adjuster_team).alias("policy_id_adjuster_team_max_adjuster_level"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_policy_id), F.lit(0)), 4).alias("adjuster_level_share_of_policy_id"),
        F.last("adjuster_level").over(w_claim_date_month_by_claim_da).alias("last_adjuster_level"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_claim_date_month_by_claim_da_2)).alias("days_since_prev_claim_date"),
        F.lead("adjuster_level", 2).over(w_adjuster_id_by_claim_date).alias("next2_adjuster_level"),
        F.row_number().over(w_adjuster_team_policy_id_by_c).alias("seq_no"),
        F.rank().over(w_all_by_claim_date_ties).alias("claim_date_rank"),
        F.rank().over(w_claim_date_month_by_claim_da_2).alias("claim_date_rank_2"),
    )
)

derived = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "adjuster_name",
        "prev_adjuster_level",
        "adjuster_level_avg_7",
        "policy_id_adjuster_team_max_adjuster_level",
        "adjuster_level_share_of_policy_id",
        "last_adjuster_level",
        "days_since_prev_claim_date",
        "seq_no",
        "claim_date_rank",
        F.month(F.col("claim_date")).alias("claim_date_month"),
        F.concat(F.col("adjuster_name"), F.lit("#"), F.col("id").cast("string")).alias("adjuster_name_ref"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

sequenced = (
    derived
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "adjuster_name",
        "prev_adjuster_level",
        "adjuster_level_avg_7",
        "policy_id_adjuster_team_max_adjuster_level",
        "adjuster_level_share_of_policy_id",
        "seq_no",
        "claim_date_rank",
        "claim_date_month",
        "adjuster_name_ref",
        "days_since_claim_date",
        F.round(F.col("claim_date_rank") / F.nullif(F.sum("claim_date_rank").over(w_adjuster_id_claim_date_month), F.lit(0)), 4).alias("claim_date_rank_share_of_adjuster_id_claim_date_"),
        F.row_number().over(w_adjuster_id_claim_date_month_2).alias("seq_no_2"),
        F.round(F.avg("last_adjuster_level").over(w_adjuster_id_claim_date_month), 2).alias("adjuster_id_claim_date_month_avg_last_adjuster_l"),
        F.lead("seq_no", 2, 0).over(w_adjuster_id_claim_date_month_3).alias("next2_seq_no"),
        F.max("days_since_prev_claim_date").over(w_adjuster_id_claim_date_month_4).alias("days_since_prev_claim_date_max_3_prior"),
    )
)

result = (
    sequenced
    .filter(F.col("adjuster_level_share_of_policy_id") <= 0.8)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "adjuster_name",
        "adjuster_level_avg_7",
        "policy_id_adjuster_team_max_adjuster_level",
        "adjuster_level_share_of_policy_id",
        "claim_date_rank",
        "claim_date_month",
        "days_since_claim_date",
        "adjuster_id_claim_date_month_avg_last_adjuster_l",
        "next2_seq_no",
        "days_since_prev_claim_date_max_3_prior",
    )
)
