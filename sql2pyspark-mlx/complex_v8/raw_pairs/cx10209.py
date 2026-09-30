from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_team_by_level = Window.partitionBy("level", "team").orderBy("level", "id")
w_team_by_level_moving_3 = Window.partitionBy("team").orderBy(F.col("level").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_team_by_level = Window.partitionBy("team").orderBy("level", "id")
w_level_team_by_level_running = Window.partitionBy("level", "team").orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level = Window.partitionBy("level")
w_team_by_level_full = Window.partitionBy("team").orderBy(F.col("level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_by_level_2 = Window.partitionBy("team").orderBy(F.col("level").asc_nulls_last(), "id")
w_team_by_level_ties = Window.partitionBy("team").orderBy("level")
w_level_by_level_decile_ties = Window.partitionBy("level").orderBy("level_decile")
w_level_level_change_by_level = Window.partitionBy("level", "level_change").orderBy("level_max_level", "id")
w_team = Window.partitionBy("team")
w_level_level_change_by_level_2 = Window.partitionBy("level", "level_change").orderBy("level_max_level")
w_first_known_level = Window.partitionBy("first_known_level")
w_level_level_change_by_level_3 = Window.partitionBy("level", "level_change").orderBy("level_max_level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_decile_by_level_vs_lev = Window.partitionBy("level_decile").orderBy("level_vs_level_avg", "id")

cleaned = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

windowed = (
    cleaned
    .select(
        "*",
        F.lead("level", 3).over(w_level_team_by_level).alias("next3_level"),
        F.sum("level").over(w_team_by_level_moving_3).alias("level_sum_4"),
        F.row_number().over(w_team_by_level).alias("seq_no"),
        F.first("level", ignorenulls=True).over(w_level_team_by_level_running).alias("first_known_level"),
        (F.col("level") - F.lag("level").over(w_team_by_level)).alias("level_change"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_level), F.lit(0)), 4).alias("level_share_of_level"),
        F.round(F.col("level") - F.avg("level").over(w_level), 2).alias("level_vs_level_avg"),
        F.nth_value("level", 2).over(w_team_by_level_full).alias("second_level"),
        F.max("level").over(w_level).alias("level_max_level"),
        F.ntile(10).over(w_team_by_level_2).alias("level_decile"),
        F.min("level").over(w_level).alias("level_min_level"),
        F.row_number().over(w_team_by_level_2).alias("seq_no_2"),
        F.round(F.cume_dist().over(w_team_by_level_ties), 3).alias("level_cume_dist"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "adjuster_name",
        "team",
        "seq_no",
        "level_change",
        "level_share_of_level",
        "level_vs_level_avg",
        "second_level",
        "level_max_level",
        "level_min_level",
        "seq_no_2",
        "level_cume_dist",
        F.dense_rank().over(w_level_by_level_decile_ties).alias("level_decile_dense_rank"),
        F.ntile(3).over(w_level_level_change_by_level).alias("level_max_level_tercile"),
        F.round(F.cume_dist().over(w_level_level_change_by_level), 3).alias("level_max_level_cume_dist"),
        F.dense_rank().over(w_level_level_change_by_level_2).alias("level_max_level_dense_rank"),
        F.round(F.col("seq_no_2") - F.avg("seq_no_2").over(w_first_known_level), 2).alias("seq_no_2_vs_first_known_level_avg"),
        F.first("level").over(w_level_level_change_by_level_3).alias("first_level"),
        F.dense_rank().over(w_level_decile_by_level_vs_lev).alias("level_vs_level_avg_dense_rank"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "adjuster_name",
        "team",
        "seq_no",
        "level_change",
        "second_level",
        "level_max_level",
        "level_min_level",
        "seq_no_2",
        "level_decile_dense_rank",
        "level_max_level_tercile",
        "level_max_level_cume_dist",
        "seq_no_2_vs_first_known_level_avg",
        "first_level",
    )
    .orderBy(F.col("team").desc_nulls_first())
)
