from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_level = Window.partitionBy("team", "level")
w_team_by_level_moving_3 = Window.partitionBy("team").orderBy("level", "id").rowsBetween(-3, Window.currentRow)
w_all_by_level = Window.orderBy(F.col("level").asc_nulls_last(), "id")
w_all_by_level_full = Window.orderBy(F.col("level").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_level_running = Window.orderBy(F.col("level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_level_ties = Window.orderBy(F.col("level").asc_nulls_last())
w_team_by_level = Window.partitionBy("team").orderBy("level", "id")
w_all_by_level_moving_1 = Window.orderBy(F.col("level").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_level_team_by_level = Window.partitionBy("level", "team").orderBy(F.col("level").desc(), "id")
w_team_level_dense_rank_by_row = Window.partitionBy("team", "level_dense_rank").orderBy(F.col("rows_in_team_level").desc(), "id")
w_level_dense_rank_2_second_le = Window.partitionBy("level_dense_rank_2", "second_level")

recent = (
    df_adjusters
    .filter((F.col("level") > 1) & (F.lower(F.col("adjuster_name")).like("%an%")))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

sequenced = (
    recent
    .select(
        "*",
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_team_level), F.lit(0)), 4).alias("level_share_of_team_level"),
        F.max("level").over(w_team_by_level_moving_3).alias("level_max_4"),
        F.dense_rank().over(w_all_by_level).alias("level_dense_rank"),
        F.nth_value("level", 2).over(w_all_by_level_full).alias("second_level"),
        F.first("level", ignorenulls=True).over(w_all_by_level_running).alias("first_known_level"),
        F.max("level").over(w_team_level).alias("team_level_max_level"),
        F.round(F.percent_rank().over(w_all_by_level_ties), 3).alias("level_percent_rank"),
        F.count(F.lit(1)).over(w_team_level).alias("rows_in_team_level"),
        F.dense_rank().over(w_all_by_level_ties).alias("level_dense_rank_2"),
        F.round(F.col("level") - F.avg("level").over(w_team_level), 2).alias("level_vs_team_level_avg"),
        F.rank().over(w_team_by_level).alias("level_rank"),
        F.round(F.avg("level").over(w_all_by_level_moving_1), 2).alias("level_avg_2"),
        F.lead("level", 2, 0).over(w_level_team_by_level).alias("next2_level"),
    )
)

windowed = (
    sequenced
    .select(
        "id",
        "adjuster_name",
        "level",
        "level_share_of_team_level",
        "second_level",
        "first_known_level",
        "team_level_max_level",
        "level_percent_rank",
        "rows_in_team_level",
        "level_dense_rank_2",
        "level_vs_team_level_avg",
        "level_rank",
        "level_avg_2",
        "next2_level",
        (F.col("level_percent_rank") - F.lag("level_percent_rank").over(w_team_level_dense_rank_by_row)).alias("level_percent_rank_change"),
        F.size(F.collect_set("team").over(w_level_dense_rank_2_second_le)).alias("distinct_team_in_level_dense_rank_2_second_level"),
        (F.col("level_max_4") - F.lag("level_max_4").over(w_team_level_dense_rank_by_row)).alias("level_max_4_change"),
        F.count(F.lit(1)).over(w_level_dense_rank_2_second_le).alias("rows_in_level_dense_rank_2_second_level"),
    )
)

result = (
    windowed
    .select(
        "id",
        "adjuster_name",
        "level",
        "level_share_of_team_level",
        "second_level",
        "first_known_level",
        "team_level_max_level",
        "rows_in_team_level",
        "level_dense_rank_2",
        "level_vs_team_level_avg",
        "next2_level",
        "level_percent_rank_change",
        "distinct_team_in_level_dense_rank_2_second_level",
        "level_max_4_change",
        "rows_in_level_dense_rank_2_second_level",
        F.round(F.col("level_percent_rank") * 0.9, 2).alias("level_percent_rank_adj"),
    )
    .orderBy(
        F.col("level_vs_team_level_avg").desc(),
        F.col("second_level").asc_nulls_last(),
        "id",
    )
    .limit(20)
)
