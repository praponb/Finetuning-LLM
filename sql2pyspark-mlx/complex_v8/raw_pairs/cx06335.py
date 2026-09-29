from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_team_by_level_moving_1 = Window.partitionBy("level", "team").orderBy("level", "id").rowsBetween(-1, Window.currentRow)
w_level_team = Window.partitionBy("level", "team")
w_all_by_level_ties = Window.orderBy("level")
w_team_level_by_level_running = Window.partitionBy("team", "level").orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_level_moving_2 = Window.orderBy(F.col("level").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_level_full = Window.orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_level = Window.orderBy("level", "id")
w_level_team_by_level = Window.partitionBy("level", "team").orderBy("level", "id")

prepared_adjusters = (
    df_adjusters
    .filter((F.lower(F.col("adjuster_name")).like("%ra%")) | (F.col("team").isNotNull()))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.substring(F.trim(F.col("adjuster_name")), 1, 3).alias("adjuster_name_prefix"),
    )
)

with_windows = (
    prepared_adjusters
    .select(
        "id",
        "team",
        "adjuster_name_prefix",
        F.round(F.avg("level").over(w_level_team_by_level_moving_1), 2).alias("level_avg_2"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_level_team), F.lit(0)), 4).alias("level_share_of_level_team"),
        F.round(F.percent_rank().over(w_all_by_level_ties), 3).alias("level_percent_rank"),
        F.first("level", ignorenulls=True).over(w_team_level_by_level_running).alias("first_known_level"),
        F.round(F.col("level") - F.avg("level").over(w_level_team), 2).alias("level_vs_level_team_avg"),
        F.round(F.avg("level").over(w_all_by_level_moving_2), 2).alias("level_avg_3"),
        F.count(F.lit(1)).over(w_level_team).alias("rows_in_level_team"),
        F.last("level").over(w_all_by_level_full).alias("last_level"),
        F.lag("level", 2).over(w_all_by_level).alias("prev2_level"),
        (F.col("level") - F.lag("level").over(w_level_team_by_level)).alias("level_change"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "team",
        "adjuster_name_prefix",
        "level_avg_2",
        "level_percent_rank",
        "first_known_level",
        "level_vs_level_team_avg",
        "level_avg_3",
        "last_level",
        "level_change",
    )
)
