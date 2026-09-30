from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_adjuster_level = Window.partitionBy("status", "adjuster_level")
w_all_by_adjuster_level_prior = Window.orderBy("adjuster_level", "id").rowsBetween(-1, -1)
w_all_by_adjuster_level_avg_1 = Window.orderBy("adjuster_level_avg_1_prior")
w_all_by_adjuster_level_avg_1_2 = Window.orderBy("adjuster_level_avg_1_prior", "id")
w_adjuster_team_status = Window.partitionBy("adjuster_team", "status")
w_all_by_policy_start_date = Window.orderBy(F.col("policy_start_date").desc(), "id")
w_all_by_adjuster_level_avg_1_3 = Window.orderBy("adjuster_level_avg_1_prior", "id").rowsBetween(-2, Window.currentRow)
w_policy_id_policy_start_date = Window.partitionBy("policy_id", "policy_start_date_rank")

filtered = (
    df_claims
    .filter((F.col("claimed_amount") >= 7204.1) & (F.col("status") == "APPROVED"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
        F.substring(F.trim(F.col("claim_type")), 1, 2).alias("claim_type_prefix"),
    )
)

combined_details_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

with_windows = (
    combined_details_adjuster
    .select(
        "id",
        "policy_id",
        "status",
        "reported_at",
        "claim_type_prefix",
        "adjuster_name",
        "adjuster_team",
        "adjuster_level",
        "policy_start_date",
        "policy_end_date",
        F.max("adjuster_level").over(w_status_adjuster_level).alias("status_adjuster_level_max_adjuster_level"),
        F.round(F.avg("adjuster_level").over(w_all_by_adjuster_level_prior), 2).alias("adjuster_level_avg_1_prior"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_adjuster_level_avg_1), 3).alias("adjuster_level_avg_1_prior_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_adjuster_level_avg_1_2), 3).alias("adjuster_level_avg_1_prior_cume_dist_2"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_adjuster_team_status), 2).alias("adjuster_level_vs_adjuster_team_status_avg"),
        F.rank().over(w_all_by_policy_start_date).alias("policy_start_date_rank"),
        F.sum("adjuster_level_avg_1_prior").over(w_all_by_adjuster_level_avg_1_3).alias("adjuster_level_avg_1_prior_sum_3"),
        F.row_number().over(w_all_by_policy_start_date).alias("policy_start_date_seq"),
    )
)

windowed = (
    with_windows_2
    .select(
        "*",
        F.sum("adjuster_level_avg_1_prior").over(w_policy_id_policy_start_date).alias("policy_id_policy_start_date_rank_sum_adjuster_le"),
        F.count(F.lit(1)).over(w_policy_id_policy_start_date).alias("rows_in_policy_id_policy_start_date_rank"),
    )
)

result = (
    windowed
    .filter(F.col("policy_start_date_seq") <= 7)
    .select(
        "id",
        "policy_id",
        "status",
        "adjuster_team",
        "adjuster_level",
        "policy_start_date",
        "adjuster_level_avg_1_prior_cume_dist_2",
        "adjuster_level_vs_adjuster_team_status_avg",
        "policy_start_date_rank",
        "adjuster_level_avg_1_prior_sum_3",
        "policy_id_policy_start_date_rank_sum_adjuster_le",
        F.round(F.col("policy_start_date_rank") / F.nullif(F.col("adjuster_level"), F.lit(0)), 3).alias("policy_start_date_rank_per_adjuster_level"),
    )
)
