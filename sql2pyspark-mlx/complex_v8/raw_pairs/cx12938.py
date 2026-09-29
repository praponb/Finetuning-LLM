from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level = Window.partitionBy("level")
w_level_by_level_full = Window.partitionBy("level").orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_team_by_level_ties = Window.partitionBy("level", "team").orderBy(F.col("level").desc())
w_level_team = Window.partitionBy("level", "team")
w_all_by_level = Window.orderBy(F.col("level").desc(), "id")
w_all_by_level_ties = Window.orderBy(F.col("level").desc())
w_level_team_by_level_full = Window.partitionBy("level", "team").orderBy(F.col("level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_team_by_level = Window.partitionBy("level", "team").orderBy("level", "id")
w_level_team_by_level_2 = Window.partitionBy("level", "team").orderBy(F.col("level").asc_nulls_last(), "id")
w_all_by_level_running = Window.orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_team_by_level_3 = Window.partitionBy("level", "team").orderBy(F.col("level").desc(), "id")
w_all_by_level_2 = Window.orderBy("level", "id")
w_level_team_by_level_ties_2 = Window.partitionBy("level", "team").orderBy(F.col("level").asc_nulls_last())
w_prev2_level = Window.partitionBy("prev2_level")
w_prev_level_3_by_last_known_l = Window.partitionBy("prev_level_3").orderBy(F.col("last_known_level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prev2_level_by_prev_level = Window.partitionBy("prev2_level").orderBy("prev_level", "id")
w_prev2_level_by_prev_level_fu = Window.partitionBy("prev2_level").orderBy("prev_level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_adjusters
    .filter(F.col("level") < 3)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("adjuster_name")), " ", "_")).alias("adjuster_name_slug"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "adjuster_name",
        "adjuster_name_slug",
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_level), F.lit(0)), 4).alias("level_share_of_level"),
        F.nth_value("level", 2).over(w_level_by_level_full).alias("second_level"),
        F.rank().over(w_level_team_by_level_ties).alias("level_rank"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_level_team), F.lit(0)), 4).alias("level_share_of_level_team"),
        F.lag("level").over(w_all_by_level).alias("prev_level"),
        F.round(F.cume_dist().over(w_all_by_level_ties), 3).alias("level_cume_dist"),
        F.first("level").over(w_level_team_by_level_full).alias("first_level"),
        F.lag("level", 2, 0).over(w_all_by_level).alias("prev2_level"),
        (F.col("level") - F.lag("level").over(w_level_team_by_level)).alias("level_change"),
        F.lag("level").over(w_level_team_by_level_2).alias("prev_level_2"),
        F.last("level", ignorenulls=True).over(w_all_by_level_running).alias("last_known_level"),
        (F.col("level") - F.lag("level").over(w_level_team_by_level_3)).alias("level_change_2"),
        (F.col("level") - F.lag("level").over(w_all_by_level_2)).alias("level_change_3"),
        F.rank().over(w_level_team_by_level_ties_2).alias("level_rank_3"),
        F.lag("level", 1, 0).over(w_level_team_by_level_3).alias("prev_level_3"),
    )
)

leaders = metrics.filter(F.col("level_rank") <= 5)

windowed = (
    leaders
    .select(
        "*",
        F.count(F.lit(1)).over(w_prev2_level).alias("rows_in_prev2_level"),
        F.first("level_rank_3").over(w_prev_level_3_by_last_known_l).alias("first_level_rank_3"),
        F.nth_value("prev2_level", 2).over(w_prev_level_3_by_last_known_l).alias("second_prev2_level"),
        F.lead("level_change_2").over(w_prev2_level_by_prev_level).alias("next_level_change_2"),
        F.nth_value("last_known_level", 2).over(w_prev2_level_by_prev_level_fu).alias("second_last_known_level"),
    )
)

result = (
    windowed
    .select(
        "id",
        "adjuster_name",
        "adjuster_name_slug",
        "level_share_of_level",
        "second_level",
        "level_rank",
        "level_share_of_level_team",
        "level_cume_dist",
        "first_level",
        "prev2_level",
        "prev_level_2",
        "level_change_2",
        "level_rank_3",
        "prev_level_3",
        "first_level_rank_3",
        "next_level_change_2",
        "second_last_known_level",
        F.length(F.trim(F.col("adjuster_name_slug"))).alias("adjuster_name_slug_len"),
        F.round(F.col("rows_in_prev2_level") / F.nullif(F.col("last_known_level"), F.lit(0)), 3).alias("rows_in_prev2_level_per_last_known_level"),
    )
    .orderBy(F.col("id").desc())
)
