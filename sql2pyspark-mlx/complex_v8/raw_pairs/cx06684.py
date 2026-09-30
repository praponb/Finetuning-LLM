from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_level_full = Window.orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_distance_team = Window.partitionBy("level_distance", "team")
w_level_distance = Window.partitionBy("level_distance")
w_all_by_level_running = Window.orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team_level_by_level_distance = Window.partitionBy("team", "level").orderBy("level_distance", "id")
w_level_distance_team_by_level = Window.partitionBy("level_distance", "team").orderBy(F.col("level").desc(), "id").rowsBetween(-1, -1)
w_all_by_level = Window.orderBy(F.col("level").desc(), "id")
w_all_by_level_ties = Window.orderBy("level")
w_last_level_by_level_distance = Window.partitionBy("last_level").orderBy(F.col("level_distance").desc(), "id").rowsBetween(-2, -1)
w_level_distance_by_level_shar = Window.partitionBy("level_distance").orderBy("level_share_of_level_distance_team", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_level_by_level_distance_2 = Window.partitionBy("last_level").orderBy(F.col("level_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_distance_by_level_shar_2 = Window.partitionBy("level_distance").orderBy("level_share_of_level_distance_team", "id")

cleaned_adjusters = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.abs(F.col("level") - 2).alias("level_distance"),
    )
)

with_trends = (
    cleaned_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "level_distance",
        F.last("level").over(w_all_by_level_full).alias("last_level"),
        F.size(F.collect_set("team").over(w_level_distance)).alias("distinct_team_in_level_distance"),
        F.round(F.col("level") - F.avg("level").over(w_level_distance), 2).alias("level_vs_level_distance_avg"),
        F.round(F.col("level") - F.avg("level").over(w_level_distance_team), 2).alias("level_vs_level_distance_team_avg"),
        F.count(F.lit(1)).over(w_all_by_level_running).alias("running_count"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_level_distance_team), F.lit(0)), 4).alias("level_share_of_level_distance_team"),
        F.lead("level", 3, 0).over(w_team_level_by_level_distance).alias("next3_level"),
        F.round(F.avg("level").over(w_level_distance_team_by_level), 2).alias("level_avg_1_prior"),
        (F.col("level") - F.lag("level").over(w_all_by_level)).alias("level_change"),
        F.round(F.percent_rank().over(w_all_by_level_ties), 3).alias("level_percent_rank"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.size(F.collect_set("team").over(w_level_distance)).alias("distinct_team_in_level_distance_2"),
        F.min("level_distance").over(w_level_distance).alias("level_distance_min_level_distance"),
        F.sum("level_percent_rank").over(w_last_level_by_level_distance).alias("level_percent_rank_sum_2_prior"),
        F.max("level_vs_level_distance_avg").over(w_level_distance_by_level_shar).alias("max_level_vs_level_distance_avg_so_far"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_level_distance), F.lit(0)), 4).alias("level_share_of_level_distance"),
        F.nth_value("level_avg_1_prior", 2).over(w_last_level_by_level_distance_2).alias("second_level_avg_1_prior"),
        F.round(F.cume_dist().over(w_level_distance_by_level_shar_2), 3).alias("level_share_of_level_distance_team_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "adjuster_name",
        "level",
        "level_distance",
        "level_vs_level_distance_avg",
        "level_vs_level_distance_team_avg",
        "running_count",
        "level_share_of_level_distance_team",
        "next3_level",
        "level_percent_rank",
        "distinct_team_in_level_distance_2",
        "max_level_vs_level_distance_avg_so_far",
        "level_share_of_level_distance",
        F.when(F.col("level_avg_1_prior") >= 3.2, "A").when(F.col("level_avg_1_prior") >= 1.8, "B").otherwise("C").alias("level_avg_1_prior_tier"),
    )
    .orderBy(
        F.col("level_vs_level_distance_avg").desc(),
        "distinct_team_in_level_distance_2",
    )
)
