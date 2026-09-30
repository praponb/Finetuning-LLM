from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level = Window.partitionBy("level")
w_all_by_level = Window.orderBy(F.col("level").desc_nulls_first(), "id")
w_all_by_level_ties = Window.orderBy(F.col("level").desc_nulls_first())
w_team_level = Window.partitionBy("team", "level")
w_all_by_level_running = Window.orderBy(F.col("level").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team_by_level_ties = Window.partitionBy("team").orderBy("level")
w_all_by_level_2 = Window.orderBy("level", "id")
w_all_by_level_ties_2 = Window.orderBy("level")
w_all_by_level_full = Window.orderBy(F.col("level").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_adjusters = (
    df_adjusters
    .filter(F.col("level") <= 3)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.concat_ws("-", F.col("adjuster_name"), F.col("team")).alias("adjuster_name_team_label"),
    )
)

windowed = (
    base_adjusters
    .select(
        "id",
        "team",
        "level",
        "adjuster_name_team_label",
        F.round(F.col("level") - F.avg("level").over(w_level), 2).alias("level_vs_level_avg"),
        (F.col("level") - F.lag("level").over(w_all_by_level)).alias("level_change"),
        F.round(F.percent_rank().over(w_all_by_level_ties), 3).alias("level_percent_rank"),
        F.lead("level", 3).over(w_all_by_level).alias("next3_level"),
        F.dense_rank().over(w_all_by_level_ties).alias("level_dense_rank"),
        F.round(F.avg("level").over(w_team_level), 2).alias("team_level_avg_level"),
        F.count(F.lit(1)).over(w_all_by_level_running).alias("running_count"),
        F.min("level").over(w_level).alias("level_min_level"),
        F.dense_rank().over(w_team_by_level_ties).alias("level_dense_rank_2"),
        F.row_number().over(w_all_by_level_2).alias("seq_no"),
        F.round(F.col("level") - F.avg("level").over(w_team_level), 2).alias("level_vs_team_level_avg"),
        F.rank().over(w_all_by_level_ties_2).alias("level_rank"),
        F.nth_value("level", 2).over(w_all_by_level_full).alias("second_level"),
    )
)

top_rows = windowed.filter((F.col("level_dense_rank") <= 2) & (F.col("level_vs_level_avg") < 2.5))

result = (
    top_rows
    .filter(F.col("level_rank") <= 7)
    .select(
        "id",
        "team",
        "level_change",
        "next3_level",
        "level_dense_rank",
        "team_level_avg_level",
        "running_count",
        "level_min_level",
        "level_dense_rank_2",
        "level_vs_team_level_avg",
        "second_level",
        F.round(F.col("level_vs_level_avg") * 1.5, 2).alias("level_vs_level_avg_adj"),
    )
    .orderBy("running_count", F.col("level_vs_team_level_avg").asc_nulls_last())
)
