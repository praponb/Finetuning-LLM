from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month = Window.partitionBy(F.date_trunc("month", "claim_date"))
w_adjuster_id_adjuster_team_by = Window.partitionBy("adjuster_id", "adjuster_team").orderBy(F.col("adjuster_level").desc(), "id")
w_adjuster_id_adjuster_team_by_2 = Window.partitionBy("adjuster_id", "adjuster_team").orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_level_by_adjuster_l = Window.partitionBy("adjuster_level").orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_level_by_adjuster_l_2 = Window.partitionBy("adjuster_level").orderBy(F.col("adjuster_level").desc(), "id")
w_claim_date_month_by_adjuster = Window.partitionBy(F.month("claim_date")).orderBy("adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_level_claim_date_mo = Window.partitionBy("adjuster_level", F.month("claim_date")).orderBy("approved_amount_adj_share_of_claim_date_month")
w_seq_no = Window.partitionBy("seq_no")
w_claim_type_second_adjuster_l = Window.partitionBy("claim_type", "second_adjuster_level")

valid = (
    df_claims
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        F.round(F.col("approved_amount") * 0.25, 2).alias("approved_amount_adj"),
    )
)

enriched = (
    valid.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.approved_amount_adj",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

windowed = (
    enriched
    .select(
        "id",
        "claim_date",
        "claim_type",
        "approved_amount_adj",
        "adjuster_team",
        "adjuster_level",
        F.round(F.col("approved_amount_adj") / F.nullif(F.sum("approved_amount_adj").over(w_claim_date_month), F.lit(0)), 4).alias("approved_amount_adj_share_of_claim_date_month"),
        F.sum("adjuster_level").over(w_claim_date_month).alias("claim_date_month_sum_adjuster_level"),
        F.row_number().over(w_adjuster_id_adjuster_team_by).alias("seq_no"),
        F.nth_value("adjuster_level", 2).over(w_adjuster_id_adjuster_team_by_2).alias("second_adjuster_level"),
        F.min("approved_amount").over(w_claim_date_month).alias("claim_date_month_min_approved_amount"),
        F.min("approved_amount_adj").over(w_claim_date_month).alias("claim_date_month_min_approved_amount_adj"),
        F.sum("approved_amount_adj").over(w_adjuster_level_by_adjuster_l).alias("running_approved_amount_adj"),
        F.lag("approved_amount").over(w_adjuster_level_by_adjuster_l_2).alias("prev_approved_amount"),
        F.min("adjuster_level").over(w_claim_date_month).alias("claim_date_month_min_adjuster_level"),
        F.last("adjuster_level").over(w_claim_date_month_by_adjuster).alias("last_adjuster_level"),
    )
)

top_per_group = windowed.filter(F.col("seq_no") == 1)

sequenced = (
    top_per_group
    .select(
        "*",
        F.dense_rank().over(w_adjuster_level_claim_date_mo).alias("approved_amount_adj_share_of_claim_date_month_de"),
        F.min("second_adjuster_level").over(w_seq_no).alias("seq_no_min_second_adjuster_level"),
        F.size(F.collect_set("adjuster_team").over(w_claim_type_second_adjuster_l)).alias("distinct_adjuster_team_in_claim_type_second_adju"),
    )
)

result = (
    sequenced
    .select("running_approved_amount_adj")
    .distinct()
)
