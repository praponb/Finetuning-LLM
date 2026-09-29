from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_team_by_reported_at = Window.partitionBy("adjuster_team").orderBy(F.col("reported_at").desc(), "id")
w_policy_id = Window.partitionBy("policy_id")
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_adjuster_id_by_reported_at_f = Window.partitionBy("adjuster_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_team_by_reported_at_2 = Window.partitionBy("adjuster_team").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount_ties = Window.orderBy(F.col("approved_amount").desc())
w_adjuster_id_by_reported_at_t = Window.partitionBy("adjuster_id").orderBy(F.col("reported_at").desc())

valid_claims = (
    df_claims
    .filter((F.year(F.col("claim_date")) == 2025) & (F.col("adjuster_id") >= 2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

with_details = (
    valid_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.rank().over(w_adjuster_team_by_reported_at).alias("reported_at_rank"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_policy_id"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id), 2).alias("approved_amount_vs_policy_id_avg"),
        F.lead("approved_amount").over(w_all_by_approved_amount).alias("next_approved_amount"),
        F.sum("approved_amount").over(w_policy_id).alias("policy_id_sum_approved_amount"),
        F.nth_value("reported_at", 2).over(w_adjuster_id_by_reported_at_f).alias("second_reported_at"),
        F.last("reported_at").over(w_adjuster_team_by_reported_at_2).alias("last_reported_at"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_adjuster_team_by_reported_at)).alias("approved_amount_change"),
        F.round(F.percent_rank().over(w_all_by_approved_amount_ties), 3).alias("approved_amount_percent_rank"),
        F.round(F.percent_rank().over(w_adjuster_id_by_reported_at_t), 3).alias("reported_at_percent_rank"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.dense_rank().over(w_all_by_approved_amount).alias("approved_amount_dense_rank"),
    )
)

top_per_group = windowed.filter(F.col("approved_amount_dense_rank") == 1)

rollup_base = (
    top_per_group
    .groupBy("adjuster_id", "status", "policy_id")
    .agg(
        F.round(F.sum("next_approved_amount") / F.nullif(F.sum("approved_amount"), F.lit(0)), 3).alias("next_approved_amount_to_approved_amount_ratio"),
        F.count("*").alias("row_count"),
        F.max("approved_amount_share_of_policy_id").alias("max_approved_amount_share_of_policy_id"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.sum(F.when(F.col("adjuster_team").isNotNull(), 1).otherwise(0)).alias("n_adjuster_team_is_not_null"),
    )
)

result = (
    rollup_base
    .select(
        "adjuster_id",
        "status",
        "next_approved_amount_to_approved_amount_ratio",
        "max_approved_amount_share_of_policy_id",
        "distinct_policy_ids",
    )
    .orderBy(F.col("max_approved_amount_share_of_policy_id").desc())
)
