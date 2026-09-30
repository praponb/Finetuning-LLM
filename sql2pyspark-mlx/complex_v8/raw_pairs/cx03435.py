from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id_by_cla = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("claimed_amount_minus_approved_amount").desc(), "id")
w_claim_date_month_adjuster_te = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_team")
w_adjuster_id_policy_id_by_cla_2 = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("claimed_amount_minus_approved_amount").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_claimed_amount_minus = Window.orderBy("claimed_amount_minus_approved_amount", "id")
w_adjuster_id_policy_id_by_cla_3 = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("claimed_amount_minus_approved_amount").desc(), "id").rowsBetween(-6, Window.currentRow)
w_adjuster_id_policy_id_by_cla_4 = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("claimed_amount_minus_approved_amount").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_all_by_prev_approved_amount = Window.orderBy(F.col("prev_approved_amount").desc(), "id")
w_all_by_approved_amount_avg_4 = Window.orderBy("approved_amount_avg_4", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_adjuster_team = Window.partitionBy("adjuster_id", "adjuster_team")
w_claim_date_month = Window.partitionBy(F.month("claim_date"))

prepared = (
    df_claims
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
    )
)

with_details_adjuster = (
    prepared.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.claimed_amount_minus_approved_amount",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

windowed = (
    with_details_adjuster
    .select(
        "*",
        F.rank().over(w_adjuster_id_policy_id_by_cla).alias("claimed_amount_minus_approved_amount_rank"),
        F.min("approved_amount").over(w_claim_date_month_adjuster_te).alias("claim_date_month_adjuster_team_min_approved_amou"),
        F.round(F.avg("approved_amount").over(w_adjuster_id_policy_id_by_cla_2), 2).alias("approved_amount_avg_4"),
        F.dense_rank().over(w_all_by_claimed_amount_minus).alias("claimed_amount_minus_approved_amount_dense_rank"),
        F.round(F.avg("approved_amount").over(w_adjuster_id_policy_id_by_cla_3), 2).alias("approved_amount_avg_7"),
        F.lag("claim_date", 3).over(w_all_by_claimed_amount_minus).alias("prev3_claim_date"),
        F.round(F.avg("approved_amount").over(w_claim_date_month_adjuster_te), 2).alias("claim_date_month_adjuster_team_avg_approved_amou"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_date_month_adjuster_te), 2).alias("approved_amount_vs_claim_date_month_adjuster_tea"),
        F.max("approved_amount").over(w_adjuster_id_policy_id_by_cla_4).alias("approved_amount_max_3"),
        F.lag("approved_amount").over(w_adjuster_id_policy_id_by_cla).alias("prev_approved_amount"),
        F.lead("claim_date").over(w_all_by_claim_date).alias("next_claim_date"),
    )
)

top_per_group = windowed.filter(F.col("claimed_amount_minus_approved_amount_rank") <= 3)

windowed_2 = (
    top_per_group
    .select(
        "id",
        "claim_type",
        "approved_amount",
        "adjuster_name",
        "adjuster_team",
        "claim_date_month_adjuster_team_min_approved_amou",
        "approved_amount_avg_4",
        "claimed_amount_minus_approved_amount_dense_rank",
        "prev3_claim_date",
        "claim_date_month_adjuster_team_avg_approved_amou",
        "approved_amount_vs_claim_date_month_adjuster_tea",
        F.lead("approved_amount").over(w_all_by_prev_approved_amount).alias("next_approved_amount"),
        F.first("prev3_claim_date").over(w_all_by_approved_amount_avg_4).alias("first_prev3_claim_date"),
        F.round(F.col("prev_approved_amount") - F.avg("prev_approved_amount").over(w_adjuster_id_adjuster_team), 2).alias("prev_approved_amount_vs_adjuster_id_adjuster_tea"),
        F.count(F.lit(1)).over(w_claim_date_month).alias("rows_in_claim_date_month"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "approved_amount",
        "approved_amount_avg_4",
        "claimed_amount_minus_approved_amount_dense_rank",
        "prev3_claim_date",
        "claim_date_month_adjuster_team_avg_approved_amou",
        "next_approved_amount",
        "prev_approved_amount_vs_adjuster_id_adjuster_tea",
        "rows_in_claim_date_month",
    )
    .orderBy("prev3_claim_date", "id")
    .limit(20)
)
