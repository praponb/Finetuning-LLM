from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_adjuster_level_adjuster_id_b = Window.partitionBy("adjuster_level", "adjuster_id").orderBy(F.col("approved_amount").desc(), "id")
w_adjuster_level_adjuster_id_b_2 = Window.partitionBy("adjuster_level", "adjuster_id").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_level = Window.partitionBy("adjuster_level")
w_reported_at_year_policy_stat = Window.partitionBy(F.year("reported_at"), "policy_status")
w_status_adjuster_level_by_app = Window.partitionBy("status", "adjuster_level").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-1, -1)
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_reported_at_tie = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc())
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_running_approved_amou = Window.orderBy(F.col("running_approved_amount").desc_nulls_first(), "id")
w_distinct_adjuster_id_in_repo = Window.partitionBy("distinct_adjuster_id_in_reported_at_year_policy_", "least_reported_at_rank_2_adjuster_level")
w_adjuster_id_distinct_adjuste = Window.partitionBy("adjuster_id", "distinct_adjuster_id_in_reported_at_year_policy_").orderBy("distinct_adjuster_id_in_reported_at_year_policy_", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_distinct_adjuste_2 = Window.partitionBy("adjuster_id", "distinct_adjuster_id_in_reported_at_year_policy_").orderBy("distinct_adjuster_id_in_reported_at_year_policy_", "id")

base = (
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
    )
)

joined = (
    base.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("a.level") > 2)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.status").alias("policy_status"),
    )
)

sequenced = (
    joined
    .select(
        "*",
        F.rank().over(w_all_by_reported_at_ties).alias("reported_at_rank"),
        F.row_number().over(w_adjuster_level_adjuster_id_b).alias("approved_amount_seq"),
        F.last("approved_amount").over(w_adjuster_level_adjuster_id_b_2).alias("last_approved_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_level), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_level"),
        F.size(F.collect_set("adjuster_id").over(w_reported_at_year_policy_stat)).alias("distinct_adjuster_id_in_reported_at_year_policy_"),
        F.sum("approved_amount").over(w_status_adjuster_level_by_app).alias("approved_amount_sum_1_prior"),
        F.ntile(10).over(w_adjuster_level_adjuster_id_b).alias("approved_amount_decile"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_adjuster_level), 2).alias("adjuster_level_vs_adjuster_level_avg"),
        F.round(F.avg("approved_amount").over(w_reported_at_year_policy_stat), 2).alias("reported_at_year_policy_status_avg_approved_amou"),
        F.sum("approved_amount").over(w_all_by_reported_at_running).alias("running_approved_amount"),
        F.max("approved_amount").over(w_reported_at_year_policy_stat).alias("reported_at_year_policy_status_max_approved_amou"),
        F.min("adjuster_level").over(w_reported_at_year_policy_stat).alias("reported_at_year_policy_status_min_adjuster_leve"),
        F.rank().over(w_policy_id_by_reported_at_tie).alias("reported_at_rank_2"),
        F.round(F.avg("approved_amount").over(w_adjuster_level), 2).alias("adjuster_level_avg_approved_amount"),
        F.count(F.lit(1)).over(w_adjuster_level).alias("rows_in_adjuster_level"),
        F.lag("reported_at").over(w_all_by_reported_at).alias("prev_reported_at"),
    )
)

annotated = (
    sequenced
    .select(
        "id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "adjuster_name",
        "adjuster_level",
        "reported_at_rank",
        "approved_amount_share_of_adjuster_level",
        "distinct_adjuster_id_in_reported_at_year_policy_",
        "approved_amount_sum_1_prior",
        "approved_amount_decile",
        "adjuster_level_vs_adjuster_level_avg",
        "running_approved_amount",
        "reported_at_year_policy_status_min_adjuster_leve",
        "reported_at_rank_2",
        "adjuster_level_avg_approved_amount",
        "prev_reported_at",
        F.round(F.col("last_approved_amount") * 1.5, 2).alias("last_approved_amount_adj"),
        F.least(F.col("reported_at_rank_2"), F.col("adjuster_level")).alias("least_reported_at_rank_2_adjuster_level"),
        F.round(F.col("approved_amount_seq") / F.nullif(F.col("running_approved_amount"), F.lit(0)), 3).alias("approved_amount_seq_per_running_approved_amount"),
    )
)

metrics = (
    annotated
    .select(
        "id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "reported_at_rank",
        "adjuster_level_vs_adjuster_level_avg",
        "running_approved_amount",
        "reported_at_year_policy_status_min_adjuster_leve",
        "reported_at_rank_2",
        "adjuster_level_avg_approved_amount",
        "prev_reported_at",
        "last_approved_amount_adj",
        "approved_amount_seq_per_running_approved_amount",
        F.sum("reported_at_rank").over(w_adjuster_id).alias("adjuster_id_sum_reported_at_rank"),
        F.lead("running_approved_amount", 1, 0).over(w_all_by_running_approved_amou).alias("next_running_approved_amount"),
        F.round(F.col("approved_amount_sum_1_prior") - F.avg("approved_amount_sum_1_prior").over(w_distinct_adjuster_id_in_repo), 2).alias("approved_amount_sum_1_prior_vs_distinct_adjuster"),
        F.round(F.col("reported_at_rank_2") - F.avg("reported_at_rank_2").over(w_distinct_adjuster_id_in_repo), 2).alias("reported_at_rank_2_vs_distinct_adjuster_id_in_re"),
        F.last("reported_at", ignorenulls=True).over(w_adjuster_id_distinct_adjuste).alias("last_known_reported_at"),
        F.row_number().over(w_adjuster_id_distinct_adjuste_2).alias("distinct_adjuster_id_in_reported_at_year_policy__2"),
        F.round(F.col("approved_amount_sum_1_prior") / F.nullif(F.sum("approved_amount_sum_1_prior").over(w_adjuster_id), F.lit(0)), 4).alias("approved_amount_sum_1_prior_share_of_adjuster_id"),
    )
)

result = (
    metrics
    .select(
        "id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "reported_at_rank",
        "adjuster_level_vs_adjuster_level_avg",
        "running_approved_amount",
        "reported_at_year_policy_status_min_adjuster_leve",
        "reported_at_rank_2",
        "adjuster_level_avg_approved_amount",
        "prev_reported_at",
        "last_approved_amount_adj",
        "approved_amount_seq_per_running_approved_amount",
        "adjuster_id_sum_reported_at_rank",
        "approved_amount_sum_1_prior_vs_distinct_adjuster",
        "reported_at_rank_2_vs_distinct_adjuster_id_in_re",
        "distinct_adjuster_id_in_reported_at_year_policy__2",
        "approved_amount_sum_1_prior_share_of_adjuster_id",
        F.abs(F.col("approved_amount") - 2349.3).alias("approved_amount_distance"),
        F.least(F.col("last_approved_amount_adj"), F.col("approved_amount")).alias("least_last_approved_amount_adj_approved_amount"),
    )
    .orderBy(F.col("reported_at_rank_2").desc())
)
