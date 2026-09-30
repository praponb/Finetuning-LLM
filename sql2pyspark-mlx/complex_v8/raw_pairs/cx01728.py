from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_adjuster_level_runnin = Window.orderBy("adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_level_adjuster_team = Window.partitionBy("adjuster_level", "adjuster_team")
w_adjuster_team_adjuster_id_by = Window.partitionBy("adjuster_team", "adjuster_id").orderBy("reported_at", "id")
w_policy_id_by_reported_at = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc_nulls_first(), "id")
w_all_by_adjuster_level_prior = Window.orderBy("adjuster_level", "id").rowsBetween(-2, -1)
w_all_by_adjuster_level_ties = Window.orderBy("adjuster_level")
w_adjuster_id_cnt_by_adjuster = Window.partitionBy("adjuster_id", "cnt").orderBy("adjuster_level_to_min_adjuster_level_so_far_rati", "adjuster_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_cnt_by_adjuster_2 = Window.partitionBy("adjuster_id", "cnt").orderBy("adjuster_level_to_min_adjuster_level_so_far_rati", "adjuster_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id = Window.partitionBy("adjuster_id")

scoped = (
    df_claims
    .filter((F.col("approved_amount") > 492.6) & (F.col("status") == "APPROVED"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
    )
)

joined = (
    scoped.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.reported_at",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.min("adjuster_level").over(w_all_by_adjuster_level_runnin).alias("min_adjuster_level_so_far"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_adjuster_level_adjuster_team), F.lit(0)), 4).alias("adjuster_level_share_of_adjuster_level_adjuster_"),
        F.row_number().over(w_adjuster_team_adjuster_id_by).alias("reported_at_seq"),
        F.lead("adjuster_level", 3, 0).over(w_policy_id_by_reported_at).alias("next3_adjuster_level"),
        F.lag("adjuster_level", 3).over(w_policy_id_by_reported_at).alias("prev3_adjuster_level"),
        F.ntile(3).over(w_adjuster_team_adjuster_id_by).alias("reported_at_tercile"),
        F.round(F.avg("adjuster_level").over(w_all_by_adjuster_level_prior), 2).alias("adjuster_level_avg_2_prior"),
        F.round(F.percent_rank().over(w_all_by_adjuster_level_ties), 3).alias("adjuster_level_percent_rank"),
    )
)

top_per_group = (
    with_windows
    .filter(F.col("reported_at_seq") <= 3)
    .drop("reported_at_seq")
)

stats = (
    top_per_group
    .groupBy("adjuster_id")
    .agg(
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.min("reported_at").alias("first_reported_at"),
        F.sum(F.when(F.upper(F.trim(F.col("adjuster_name"))).like("B%"), F.col("adjuster_level_percent_rank"))).alias("adjuster_level_percent_rank_upper_trim_adjuster_"),
        F.count("*").alias("cnt"),
        F.round(F.sum("adjuster_level") / F.nullif(F.sum("min_adjuster_level_so_far"), F.lit(0)), 3).alias("adjuster_level_to_min_adjuster_level_so_far_rati"),
        F.max("adjuster_level_share_of_adjuster_level_adjuster_").alias("max_adjuster_level_share_of_adjuster_level_adjus"),
    )
    .filter(F.col("max_adjuster_level_share_of_adjuster_level_adjus") > 0.1)
    .drop("max_adjuster_level_share_of_adjuster_level_adjus")
)

with_windows_2 = (
    stats
    .select(
        "adjuster_id",
        "distinct_adjuster_ids",
        "first_reported_at",
        "adjuster_level_percent_rank_upper_trim_adjuster_",
        "cnt",
        "adjuster_level_to_min_adjuster_level_so_far_rati",
        F.last("cnt", ignorenulls=True).over(w_adjuster_id_cnt_by_adjuster).alias("last_known_cnt"),
        F.min("cnt").over(w_adjuster_id_cnt_by_adjuster).alias("min_cnt_so_far"),
        F.nth_value("cnt", 2).over(w_adjuster_id_cnt_by_adjuster_2).alias("second_cnt"),
        F.round(F.col("distinct_adjuster_ids") / F.nullif(F.sum("distinct_adjuster_ids").over(w_adjuster_id), F.lit(0)), 4).alias("distinct_adjuster_ids_share_of_adjuster_id"),
    )
)

result = (
    with_windows_2
    .filter(F.col("distinct_adjuster_ids_share_of_adjuster_id") < 0.6)
    .select(
        "adjuster_id",
        "distinct_adjuster_ids",
        "cnt",
        "adjuster_level_to_min_adjuster_level_so_far_rati",
        "last_known_cnt",
        "min_cnt_so_far",
        "second_cnt",
        F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"),
    )
    .orderBy(F.col("cnt_filled").asc(), "adjuster_id")
    .limit(20)
)
