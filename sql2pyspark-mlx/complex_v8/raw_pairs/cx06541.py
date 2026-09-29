from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_level = Window.partitionBy("team", "level")
w_level_team = Window.partitionBy("level", "team")
w_all_by_level = Window.orderBy("level", "id")
w_all_by_level_moving_6 = Window.orderBy("level", "id").rowsBetween(-6, Window.currentRow)
w_level_team_by_level_full = Window.partitionBy("level", "team").orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_by_level_ties = Window.partitionBy("team").orderBy(F.col("level").asc_nulls_last())
w_level_last_level = Window.partitionBy("level", "last_level")
w_level_dense_rank_2_by_level = Window.partitionBy("level_dense_rank_2").orderBy(F.col("level_avg_7").desc(), "id").rowsBetween(-1, -1)
w_all_by_team_level_min_level = Window.orderBy("team_level_min_level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_team_level_min_level_2 = Window.orderBy("team_level_min_level", "id")

scoped = (
    df_adjusters
    .filter(F.col("level") >= 2)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug"),
    )
)

windowed = (
    scoped
    .select(
        "*",
        F.count(F.lit(1)).over(w_team_level).alias("rows_in_team_level"),
        F.min("level").over(w_team_level).alias("team_level_min_level"),
        F.count(F.lit(1)).over(w_level_team).alias("rows_in_level_team"),
        F.max("level").over(w_team_level).alias("team_level_max_level"),
        F.row_number().over(w_all_by_level).alias("rn"),
        F.round(F.avg("level").over(w_all_by_level_moving_6), 2).alias("level_avg_7"),
        F.dense_rank().over(w_all_by_level).alias("level_dense_rank"),
        F.last("level").over(w_level_team_by_level_full).alias("last_level"),
        F.dense_rank().over(w_team_by_level_ties).alias("level_dense_rank_2"),
    )
)

top_per_group = (
    windowed
    .filter((F.col("level_dense_rank") <= 5) & (F.col("rn") < 8))
    .drop("level_dense_rank")
)

metrics = (
    top_per_group
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "team_slug",
        "rows_in_team_level",
        "team_level_min_level",
        "rows_in_level_team",
        "team_level_max_level",
        "level_dense_rank_2",
        F.count(F.lit(1)).over(w_level_last_level).alias("rows_in_level_last_level"),
        F.sum("team_level_min_level").over(w_level_dense_rank_2_by_level).alias("team_level_min_level_sum_1_prior"),
        F.sum("level_dense_rank_2").over(w_level_last_level).alias("level_last_level_sum_level_dense_rank_2"),
        F.nth_value("team_level_min_level", 2).over(w_all_by_team_level_min_level).alias("second_team_level_min_level"),
        F.ntile(10).over(w_all_by_team_level_min_level_2).alias("team_level_min_level_decile"),
    )
)

result = (
    metrics
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "team_slug",
        "rows_in_team_level",
        "team_level_min_level",
        "rows_in_level_team",
        "team_level_max_level",
        "rows_in_level_last_level",
        "level_last_level_sum_level_dense_rank_2",
        "second_team_level_min_level",
        "team_level_min_level_decile",
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        F.round(F.col("rows_in_level_team") / F.nullif(F.col("rows_in_level_last_level"), F.lit(0)), 3).alias("rows_in_level_team_per_rows_in_level_last_level"),
    )
    .orderBy("level", F.col("team_level_min_level_decile").asc_nulls_last())
)
