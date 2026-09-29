from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_adjuster_team = Window.partitionBy("adjuster_id", "adjuster_team")
w_adjuster_id_reported_at_to_d = Window.partitionBy("adjuster_id", F.to_date("reported_at")).orderBy(F.col("reported_at_hour").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_date_prior_6 = Window.orderBy("claim_date", "id").rowsBetween(-6, -1)
w_adjuster_id_by_adjuster_leve = Window.partitionBy("adjuster_id").orderBy("adjuster_level")
w_policy_id_by_reported_at_pri = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(-3, -1)
w_policy_id_by_reported_at = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), "id")
w_adjuster_id_reported_at_to_d_2 = Window.partitionBy("adjuster_id", F.to_date("reported_at")).orderBy(F.col("reported_at_hour").desc_nulls_first(), "id")
w_all_by_claim_date_ties = Window.orderBy("claim_date")
w_adjuster_team_claim_date_yea = Window.partitionBy("adjuster_team", F.year("claim_date")).orderBy("adjuster_level_vs_adjuster_id_adjuster_team_avg")
w_reported_at_decile_status = Window.partitionBy("reported_at_decile", "status")
w_all_by_rows_in_adjuster_id_a = Window.orderBy(F.col("rows_in_adjuster_id_adjuster_team").desc_nulls_first(), "id")

filtered = (
    df_claims
    .filter((F.col("claimed_amount") < 39666.5) & (F.col("claim_date") >= F.lit("2024-06-07").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
        F.date_trunc("hour", F.col("reported_at")).alias("reported_at_hour"),
    )
)

with_details_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.*",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

with_windows = (
    with_details_adjuster
    .select(
        "*",
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_adjuster_id_adjuster_team), 2).alias("adjuster_level_vs_adjuster_id_adjuster_team_avg"),
        F.nth_value("claim_date", 2).over(w_adjuster_id_reported_at_to_d).alias("second_claim_date"),
        F.round(F.avg("adjuster_level").over(w_all_by_claim_date_prior_6), 2).alias("adjuster_level_avg_6_prior"),
        F.round(F.percent_rank().over(w_adjuster_id_by_adjuster_leve), 3).alias("adjuster_level_percent_rank"),
        F.round(F.avg("adjuster_level").over(w_policy_id_by_reported_at_pri), 2).alias("adjuster_level_avg_3_prior"),
        F.ntile(10).over(w_policy_id_by_reported_at).alias("reported_at_decile"),
        F.count(F.lit(1)).over(w_adjuster_id_adjuster_team).alias("rows_in_adjuster_id_adjuster_team"),
        F.lag("reported_at", 2).over(w_adjuster_id_reported_at_to_d_2).alias("prev2_reported_at"),
        F.dense_rank().over(w_all_by_claim_date_ties).alias("claim_date_dense_rank"),
    )
)

top_per_group = (
    with_windows
    .filter((F.col("claim_date_dense_rank") == 1) & (~F.col("adjuster_team").isin("Travel", "Property")))
)

ranked = (
    top_per_group
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "reported_at",
        "reported_at_hour",
        "adjuster_level",
        "adjuster_name",
        "adjuster_team",
        "adjuster_level_vs_adjuster_id_adjuster_team_avg",
        "second_claim_date",
        "adjuster_level_avg_6_prior",
        "adjuster_level_percent_rank",
        "adjuster_level_avg_3_prior",
        "reported_at_decile",
        "rows_in_adjuster_id_adjuster_team",
        F.rank().over(w_adjuster_team_claim_date_yea).alias("adjuster_level_vs_adjuster_id_adjuster_team_avg_"),
        F.size(F.collect_set("adjuster_id").over(w_reported_at_decile_status)).alias("distinct_adjuster_id_in_reported_at_decile_statu"),
        F.round(F.col("adjuster_level_percent_rank") - F.avg("adjuster_level_percent_rank").over(w_reported_at_decile_status), 2).alias("adjuster_level_percent_rank_vs_reported_at_decil"),
        F.count(F.lit(1)).over(w_reported_at_decile_status).alias("rows_in_reported_at_decile_status"),
        F.round(F.avg("adjuster_level_avg_3_prior").over(w_reported_at_decile_status), 2).alias("reported_at_decile_status_avg_adjuster_level_avg"),
        F.round(F.cume_dist().over(w_all_by_rows_in_adjuster_id_a), 3).alias("rows_in_adjuster_id_adjuster_team_cume_dist"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_rows_in_adjuster_id_a)).alias("days_since_prev_claim_date"),
    )
)

per_group = (
    ranked
    .groupBy(
        "adjuster_level_vs_adjuster_id_adjuster_team_avg_",
        "adjuster_level",
        "adjuster_team",
    )
    .agg(
        F.sum("adjuster_level_vs_adjuster_id_adjuster_team_avg_").alias("total_adjuster_level_vs_adjuster_id_adjuster_tea"),
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.round(F.sum("distinct_adjuster_id_in_reported_at_decile_statu") / F.nullif(F.sum("rows_in_reported_at_decile_status"), F.lit(0)), 3).alias("distinct_adjuster_id_in_reported_at_decile_statu_2"),
        F.max("rows_in_reported_at_decile_status").alias("max_rows_in_reported_at_decile_status_check"),
    )
    .filter(F.col("max_rows_in_reported_at_decile_status_check") > 8)
    .drop("max_rows_in_reported_at_decile_status_check")
)

result = (
    per_group
    .select(
        "adjuster_level_vs_adjuster_id_adjuster_team_avg_",
        "adjuster_level",
        "adjuster_team",
        "adjuster_name_list",
        "distinct_adjuster_id_in_reported_at_decile_statu_2",
    )
    .orderBy(
        F.col("adjuster_team").desc_nulls_first(),
        "distinct_adjuster_id_in_reported_at_decile_statu_2",
    )
)
