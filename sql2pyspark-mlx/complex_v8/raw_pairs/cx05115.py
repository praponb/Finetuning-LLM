from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_team_adjuster_level = Window.partitionBy("adjuster_team", "adjuster_level")
w_all_by_adjuster_level = Window.orderBy(F.col("adjuster_level").desc(), "id")
w_all_by_approved_amount_runni = Window.orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_approved_amount_movin = Window.orderBy("approved_amount", "id").rowsBetween(-6, Window.currentRow)
w_status_adjuster_team = Window.partitionBy("status", "adjuster_team")
w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy("approved_amount", "id")
w_adjuster_level = Window.partitionBy("adjuster_level")
w_all_by_adjuster_level_tercil = Window.orderBy("adjuster_level_tercile", "id")
w_all_by_adjuster_level_change = Window.orderBy("adjuster_level_change", "id").rowsBetween(-1, -1)

valid = (
    df_claims
    .filter((F.col("claimed_amount") >= 17759.3) | (F.col("claim_date").between(F.lit("2024-02-05").cast("date"), F.lit("2025-10-30").cast("date"))))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
    )
)

enriched = (
    valid.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "status",
        "adjuster_team",
        "adjuster_name",
        "adjuster_level",
        F.size(F.collect_set("policy_id").over(w_adjuster_team_adjuster_level)).alias("distinct_policy_id_in_adjuster_team_adjuster_lev"),
        F.ntile(3).over(w_all_by_adjuster_level).alias("adjuster_level_tercile"),
        F.sum("adjuster_level").over(w_adjuster_team_adjuster_level).alias("adjuster_team_adjuster_level_sum_adjuster_level"),
        F.first("approved_amount", ignorenulls=True).over(w_all_by_approved_amount_runni).alias("first_known_approved_amount"),
        F.sum("approved_amount").over(w_all_by_approved_amount_movin).alias("approved_amount_sum_7"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_status_adjuster_team), F.lit(0)), 4).alias("adjuster_level_share_of_status_adjuster_team"),
        F.size(F.collect_set("policy_id").over(w_status_adjuster_team)).alias("distinct_policy_id_in_status_adjuster_team"),
        F.round(F.cume_dist().over(w_policy_id_by_approved_amount), 3).alias("approved_amount_cume_dist"),
        F.size(F.collect_set("policy_id").over(w_adjuster_level)).alias("distinct_policy_id_in_adjuster_level"),
        (F.col("adjuster_level") - F.lag("adjuster_level").over(w_policy_id_by_approved_amount)).alias("adjuster_level_change"),
    )
)

ranked = (
    with_windows
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "status",
        "adjuster_team",
        "adjuster_name",
        "adjuster_level",
        "distinct_policy_id_in_adjuster_team_adjuster_lev",
        "adjuster_level_tercile",
        "adjuster_team_adjuster_level_sum_adjuster_level",
        "first_known_approved_amount",
        "adjuster_level_share_of_status_adjuster_team",
        "distinct_policy_id_in_adjuster_level",
        F.lead("adjuster_level_change").over(w_all_by_adjuster_level_tercil).alias("next_adjuster_level_change"),
        F.row_number().over(w_all_by_adjuster_level_tercil).alias("adjuster_level_tercile_seq"),
        F.max("approved_amount").over(w_all_by_adjuster_level_change).alias("approved_amount_max_1_prior"),
    )
)

summary = (
    ranked
    .filter(F.col("adjuster_level_tercile") < 6)
    .groupBy("distinct_policy_id_in_adjuster_team_adjuster_lev")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("approved_amount"), 2).alias("avg_approved_amount"),
        F.count(F.when(~F.col("status").isin("IN_REVIEW", "OPEN"), 1)).alias("n_status_not_in_in_review_open"),
        F.round(F.avg("distinct_policy_id_in_adjuster_level"), 2).alias("avg_distinct_policy_id_in_adjuster_level"),
        F.count(F.when(F.upper(F.trim(F.col("adjuster_name"))).like("A%"), 1)).alias("n_upper_trim_adjuster_name_lik"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    summary
    .select(
        "distinct_policy_id_in_adjuster_team_adjuster_lev",
        "row_count",
        "avg_approved_amount",
        "n_status_not_in_in_review_open",
        "avg_distinct_policy_id_in_adjuster_level",
        "n_upper_trim_adjuster_name_lik",
        "distinct_ids",
        F.abs(F.col("row_count") - 8).alias("row_count_distance"),
    )
    .orderBy(F.col("avg_distinct_policy_id_in_adjuster_level").asc_nulls_last())
)
