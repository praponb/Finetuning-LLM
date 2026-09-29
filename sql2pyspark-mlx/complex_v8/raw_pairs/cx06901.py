from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_level_by_level_running = Window.partitionBy("team", "level").orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_level_running = Window.orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team_level_by_level_full = Window.partitionBy("team", "level").orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_level = Window.partitionBy("team", "level")
w_team = Window.partitionBy("team")
w_team_level_by_level = Window.partitionBy("team", "level").orderBy("level", "id")
w_all_by_level_full = Window.orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_level = Window.orderBy(F.col("level").asc_nulls_last(), "id")
w_team_level_by_level_full_2 = Window.partitionBy("team", "level").orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_team_by_level_ties = Window.partitionBy("level", "team").orderBy("level")
w_level = Window.partitionBy("level")
w_level_team_by_level = Window.partitionBy("level", "team").orderBy("level", "id")
w_last_known_level_2_next2_lev = Window.partitionBy("last_known_level_2", "next2_level")
w_level_last_known_level = Window.partitionBy("level", "last_known_level")
w_all_by_level_vs_level_avg_pr = Window.orderBy("level_vs_level_avg", "id").rowsBetween(-2, -1)
w_last_known_level_last_known = Window.partitionBy("last_known_level", "last_known_level_2").orderBy("level_percent_rank", "id").rowsBetween(-1, -1)
w_all_by_level_vs_level_avg_ru = Window.orderBy("level_vs_level_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_adjusters = (
    df_adjusters
    .filter((F.length(F.trim(F.col("adjuster_name"))) >= 4) & (F.col("team") == "Health"))
)

with_windows = (
    valid_adjusters
    .select(
        "*",
        F.last("level", ignorenulls=True).over(w_team_level_by_level_running).alias("last_known_level"),
        F.last("level", ignorenulls=True).over(w_all_by_level_running).alias("last_known_level_2"),
        F.first("level").over(w_team_level_by_level_full).alias("first_level"),
        F.count(F.lit(1)).over(w_team_level).alias("rows_in_team_level"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_team), F.lit(0)), 4).alias("level_share_of_team"),
        F.round(F.percent_rank().over(w_team_level_by_level), 3).alias("level_percent_rank"),
        F.nth_value("level", 2).over(w_all_by_level_full).alias("second_level"),
        F.ntile(3).over(w_team_level_by_level).alias("level_tercile"),
        F.round(F.col("level") - F.avg("level").over(w_team), 2).alias("level_vs_team_avg"),
        (F.col("level") - F.lag("level").over(w_all_by_level)).alias("level_change"),
        F.nth_value("level", 2).over(w_team_level_by_level_full_2).alias("second_level_2"),
        F.lead("level", 2).over(w_all_by_level).alias("next2_level"),
        F.rank().over(w_level_team_by_level_ties).alias("level_rank"),
        F.round(F.col("level") - F.avg("level").over(w_level), 2).alias("level_vs_level_avg"),
        F.round(F.percent_rank().over(w_level_team_by_level), 3).alias("level_percent_rank_2"),
    )
)

top_rows = (
    with_windows
    .filter((F.col("level_rank") == 1) & (F.col("level_vs_team_avg") >= -0.5))
    .drop("level_rank")
)

with_windows_2 = (
    top_rows
    .select(
        "*",
        F.round(F.col("level_tercile") / F.nullif(F.sum("level_tercile").over(w_last_known_level_2_next2_lev), F.lit(0)), 4).alias("level_tercile_share_of_last_known_level_2_next2_"),
        F.count(F.lit(1)).over(w_last_known_level_2_next2_lev).alias("rows_in_last_known_level_2_next2_level"),
        F.sum("second_level").over(w_level_last_known_level).alias("level_last_known_level_sum_second_level"),
        F.round(F.avg("level_tercile").over(w_all_by_level_vs_level_avg_pr), 2).alias("level_tercile_avg_2_prior"),
        F.sum("next2_level").over(w_last_known_level_last_known).alias("next2_level_sum_1_prior"),
        F.last("level_share_of_team", ignorenulls=True).over(w_all_by_level_vs_level_avg_ru).alias("last_known_level_share_of_team"),
        F.round(F.avg("level_tercile").over(w_level_last_known_level), 2).alias("level_last_known_level_avg_level_tercile"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "adjuster_name",
        "team",
        "last_known_level_2",
        "first_level",
        "level_share_of_team",
        "next2_level",
        "level_vs_level_avg",
        "level_percent_rank_2",
        "rows_in_last_known_level_2_next2_level",
        "level_last_known_level_sum_second_level",
        "level_tercile_avg_2_prior",
        "next2_level_sum_1_prior",
        "last_known_level_share_of_team",
    )
    .orderBy("last_known_level_2", "next2_level")
)
