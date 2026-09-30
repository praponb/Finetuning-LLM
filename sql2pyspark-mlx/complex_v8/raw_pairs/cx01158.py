from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team = Window.partitionBy("team")
w_level = Window.partitionBy("level")
w_all_by_level_distance_prior = Window.orderBy(F.col("level_distance").desc(), "id").rowsBetween(-6, -1)
w_all_by_level_moving_6 = Window.orderBy("level", "id").rowsBetween(-6, Window.currentRow)
w_all_by_level = Window.orderBy("level", "id")
w_team_level_by_level_full = Window.partitionBy("team", "level").orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_adjusters
    .filter(F.col("team").isin("Motor", "Health", "Property"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.concat_ws(": ", F.col("adjuster_name"), F.col("team")).alias("adjuster_name_team_label"),
        F.abs(F.col("level") - 3).alias("level_distance"),
    )
)

ranked = (
    filtered
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "adjuster_name_team_label",
        "level_distance",
        F.count(F.lit(1)).over(w_team).alias("rows_in_team"),
        F.round(F.col("level") - F.avg("level").over(w_level), 2).alias("level_vs_level_avg"),
        F.round(F.avg("level_distance").over(w_all_by_level_distance_prior), 2).alias("level_distance_avg_6_prior"),
        F.sum("level_distance").over(w_level).alias("level_sum_level_distance"),
        F.min("level").over(w_team).alias("team_min_level"),
        F.round(F.avg("level_distance").over(w_all_by_level_moving_6), 2).alias("level_distance_avg_7"),
        F.size(F.collect_set("team").over(w_level)).alias("distinct_team_in_level"),
        F.rank().over(w_all_by_level).alias("level_rank"),
        F.nth_value("level", 2).over(w_team_level_by_level_full).alias("second_level"),
        F.lag("level", 1, 0).over(w_all_by_level).alias("prev_level"),
        F.round(F.col("level_distance") / F.nullif(F.sum("level_distance").over(w_team), F.lit(0)), 4).alias("level_distance_share_of_team"),
    )
)

top_per_group = ranked.filter(F.col("level_rank") <= 2)

result = (
    top_per_group
    .select(
        "id",
        "team",
        "level",
        "adjuster_name_team_label",
        "level_distance",
        "rows_in_team",
        "level_vs_level_avg",
        "level_sum_level_distance",
        "team_min_level",
        "level_distance_avg_7",
        "distinct_team_in_level",
        "level_rank",
        "second_level",
        "prev_level",
        "level_distance_share_of_team",
        F.concat(F.col("adjuster_name_team_label"), F.lit("#"), F.col("prev_level").cast("string")).alias("adjuster_name_team_label_ref"),
    )
    .orderBy("id")
)
