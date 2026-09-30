from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_level_policy_id_by = Window.partitionBy("adjuster_level", "policy_id").orderBy(F.col("claimed_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_level = Window.partitionBy("adjuster_level")
w_claim_type_adjuster_id = Window.partitionBy("claim_type", "adjuster_id")
w_adjuster_level_policy_id_by_2 = Window.partitionBy("adjuster_level", "policy_id").orderBy(F.col("claimed_amount").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_all_by_claimed_amount_ties = Window.orderBy("claimed_amount")
w_all_by_approved_amount_per_c = Window.orderBy("approved_amount_per_claimed_amount", "id")
w_adjuster_level_policy_id_by_3 = Window.partitionBy("adjuster_level", "policy_id").orderBy(F.col("claimed_amount").desc_nulls_first())
w_all_by_approved_amount_per_c_2 = Window.orderBy("approved_amount_per_claimed_amount", "id").rowsBetween(-1, -1)
w_adjuster_id_adjuster_level_b = Window.partitionBy("adjuster_id", "adjuster_level").orderBy("adjuster_level", "id").rowsBetween(-3, Window.currentRow)
w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy("claimed_amount", "id")
w_all_by_claimed_amount_dense = Window.orderBy(F.col("claimed_amount_dense_rank").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_team_by_adjuster_le = Window.partitionBy("adjuster_team").orderBy("adjuster_level_avg_4", "id")
w_distinct_claim_type_in_adjus = Window.partitionBy("distinct_claim_type_in_adjuster_level").orderBy("adjuster_level_avg_4", "id")
w_distinct_claim_type_in_adjus_2 = Window.partitionBy("distinct_claim_type_in_adjuster_level")
w_adjuster_team_by_adjuster_le_2 = Window.partitionBy("adjuster_team").orderBy("adjuster_level_avg_4", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_team = Window.partitionBy("adjuster_team")
w_distinct_adjuster_teams_dist = Window.partitionBy("distinct_adjuster_teams", "distinct_adjuster_ids")

base = (
    df_claims
    .filter(F.col("approved_amount") <= 20880.4)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
    )
)

enriched_adjuster = (
    base.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount_per_claimed_amount",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

metrics = (
    enriched_adjuster
    .select(
        "*",
        F.nth_value("adjuster_level", 2).over(w_adjuster_level_policy_id_by).alias("second_adjuster_level"),
        F.size(F.collect_set("claim_type").over(w_adjuster_level)).alias("distinct_claim_type_in_adjuster_level"),
        F.round(F.avg("claimed_amount").over(w_claim_type_adjuster_id), 2).alias("claim_type_adjuster_id_avg_claimed_amount"),
        F.round(F.avg("adjuster_level").over(w_adjuster_level_policy_id_by_2), 2).alias("adjuster_level_avg_3_prior"),
        F.round(F.percent_rank().over(w_all_by_claimed_amount_ties), 3).alias("claimed_amount_percent_rank"),
        F.round(F.avg("claimed_amount").over(w_adjuster_level), 2).alias("adjuster_level_avg_claimed_amount"),
        F.rank().over(w_all_by_approved_amount_per_c).alias("approved_amount_per_claimed_amount_rank"),
        F.dense_rank().over(w_adjuster_level_policy_id_by_3).alias("claimed_amount_dense_rank"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_adjuster_level), F.lit(0)), 4).alias("adjuster_level_share_of_adjuster_level"),
        F.max("claimed_amount").over(w_all_by_approved_amount_per_c_2).alias("claimed_amount_max_1_prior"),
        F.count(F.lit(1)).over(w_claim_type_adjuster_id).alias("rows_in_claim_type_adjuster_id"),
        F.round(F.avg("adjuster_level").over(w_adjuster_id_adjuster_level_b), 2).alias("adjuster_level_avg_4"),
        F.last("claimed_amount").over(w_adjuster_level_policy_id_by).alias("last_claimed_amount"),
        F.round(F.percent_rank().over(w_claim_type_by_claimed_amount), 3).alias("claimed_amount_percent_rank_2"),
        F.min("adjuster_level").over(w_claim_type_adjuster_id).alias("claim_type_adjuster_id_min_adjuster_level"),
    )
)

top_per_group = (
    metrics
    .filter(F.col("approved_amount_per_claimed_amount_rank") == 1)
    .drop("approved_amount_per_claimed_amount_rank")
)

metrics_2 = (
    top_per_group
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "adjuster_team",
        "adjuster_name",
        "distinct_claim_type_in_adjuster_level",
        "claim_type_adjuster_id_avg_claimed_amount",
        "adjuster_level_avg_claimed_amount",
        "claimed_amount_dense_rank",
        "adjuster_level_share_of_adjuster_level",
        "rows_in_claim_type_adjuster_id",
        "last_claimed_amount",
        F.min("last_claimed_amount").over(w_all_by_claimed_amount_dense).alias("min_last_claimed_amount_so_far"),
        (F.col("adjuster_level") - F.lag("adjuster_level").over(w_adjuster_team_by_adjuster_le)).alias("adjuster_level_change"),
        F.round(F.percent_rank().over(w_distinct_claim_type_in_adjus), 3).alias("adjuster_level_avg_4_percent_rank"),
        F.sum("adjuster_level").over(w_distinct_claim_type_in_adjus_2).alias("distinct_claim_type_in_adjuster_level_sum_adjust"),
        F.nth_value("adjuster_level_avg_4", 2).over(w_adjuster_team_by_adjuster_le_2).alias("second_adjuster_level_avg_4"),
        F.min("claimed_amount_dense_rank").over(w_distinct_claim_type_in_adjus_2).alias("distinct_claim_type_in_adjuster_level_min_claime"),
        F.sum("adjuster_level_avg_3_prior").over(w_adjuster_team).alias("adjuster_team_sum_adjuster_level_avg_3_prior"),
    )
)

per_group = (
    metrics_2
    .groupBy(
        F.when(F.col("last_claimed_amount") >= 33690.0, "A").when(F.col("last_claimed_amount") >= 13635.1, "B").otherwise("C").alias("last_claimed_amount_tier"),
        "adjuster_level_change",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.round(F.sum("adjuster_level_avg_4_percent_rank") / F.nullif(F.sum("distinct_claim_type_in_adjuster_level_min_claime"), F.lit(0)), 3).alias("adjuster_level_avg_4_percent_rank_to_distinct_cl"),
        F.countDistinct("adjuster_team").alias("distinct_adjuster_teams"),
        F.avg("adjuster_level_share_of_adjuster_level").alias("avg_adjuster_level_share_of_adjuster_level_check"),
    )
    .filter(F.col("avg_adjuster_level_share_of_adjuster_level_check") > 0.2)
    .drop("avg_adjuster_level_share_of_adjuster_level_check")
)

ranked = (
    per_group
    .select(
        "last_claimed_amount_tier",
        "adjuster_level_change",
        "distinct_policy_ids",
        "distinct_adjuster_ids",
        "distinct_adjuster_teams",
        F.round(F.col("distinct_policy_ids") - F.avg("distinct_policy_ids").over(w_distinct_adjuster_teams_dist), 2).alias("distinct_policy_ids_vs_distinct_adjuster_teams_d"),
        F.size(F.collect_set("last_claimed_amount_tier").over(w_distinct_adjuster_teams_dist)).alias("distinct_last_claimed_amount_tier_in_distinct_ad"),
    )
)

result = (
    ranked
    .filter(F.col("distinct_policy_ids") < 9)
    .select(
        "last_claimed_amount_tier",
        "adjuster_level_change",
        "distinct_policy_ids",
        "distinct_adjuster_ids",
        "distinct_adjuster_teams",
        "distinct_last_claimed_amount_tier_in_distinct_ad",
        F.when(F.col("distinct_policy_ids") >= 9, "A").when(F.col("distinct_policy_ids") >= 4, "B").otherwise("C").alias("distinct_policy_ids_tier"),
        F.abs(F.col("adjuster_level_change") - 0).alias("adjuster_level_change_distance"),
    )
    .orderBy(F.col("adjuster_level_change").desc())
)
