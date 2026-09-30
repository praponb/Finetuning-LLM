from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_level = Window.partitionBy("team", "level")
w_team_level_by_level = Window.partitionBy("team", "level").orderBy(F.col("level").desc(), "id")
w_team_by_level_ties = Window.partitionBy("team").orderBy("level")
w_all_by_level = Window.orderBy(F.col("level").desc(), "id")
w_team_by_level = Window.partitionBy("team").orderBy("level", "id")
w_team_level_by_level_prior_6 = Window.partitionBy("team", "level").orderBy(F.col("level").desc(), "id").rowsBetween(-6, -1)
w_all_by_level_running = Window.orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team_level_by_level_running = Window.partitionBy("team", "level").orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_prev3_level_level_dense_rank = Window.partitionBy("prev3_level", "level_dense_rank_tier")
w_level_change_3_by_level_sum = Window.partitionBy("level_change_3").orderBy(F.col("level_sum_6_prior").desc(), "id")
w_level_change_level_dense_ran = Window.partitionBy("level_change", "level_dense_rank_tier").orderBy(F.col("level_percent_rank").asc_nulls_last(), "id").rowsBetween(-3, -1)

valid = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

metrics = (
    valid
    .select(
        "id",
        F.min("level").over(w_team_level).alias("team_level_min_level"),
        F.row_number().over(w_team_level_by_level).alias("level_seq"),
        F.dense_rank().over(w_team_by_level_ties).alias("level_dense_rank"),
        (F.col("level") - F.lag("level").over(w_all_by_level)).alias("level_change"),
        F.lag("level", 3).over(w_team_by_level).alias("prev3_level"),
        F.count(F.lit(1)).over(w_team_level).alias("rows_in_team_level"),
        F.round(F.percent_rank().over(w_team_by_level), 3).alias("level_percent_rank"),
        F.dense_rank().over(w_team_by_level).alias("level_dense_rank_3"),
        (F.col("level") - F.lag("level").over(w_team_by_level)).alias("level_change_2"),
        (F.col("level") - F.lag("level").over(w_team_level_by_level)).alias("level_change_3"),
        F.round(F.avg("level").over(w_team_level), 2).alias("team_level_avg_level"),
        F.sum("level").over(w_team_level_by_level_prior_6).alias("level_sum_6_prior"),
        F.count(F.lit(1)).over(w_all_by_level_running).alias("running_count"),
        F.sum("level").over(w_team_level_by_level_running).alias("running_level"),
    )
)

scored = (
    metrics
    .select(
        "*",
        (F.col("level_dense_rank_3") + F.col("level_dense_rank")).alias("level_dense_rank_3_plus_level_dense_rank"),
        F.abs(F.col("rows_in_team_level") - 11).alias("rows_in_team_level_distance"),
        F.when(F.col("level_dense_rank") >= 8, "HIGH").when(F.col("level_dense_rank") >= 4, "MEDIUM").otherwise("LOW").alias("level_dense_rank_tier"),
    )
)

sequenced = (
    scored
    .select(
        "*",
        F.round(F.col("level_change_3") - F.avg("level_change_3").over(w_prev3_level_level_dense_rank), 2).alias("level_change_3_vs_prev3_level_level_dense_rank_t"),
        F.lead("level_change_3", 2).over(w_level_change_3_by_level_sum).alias("next2_level_change_3"),
        F.round(F.avg("prev3_level").over(w_level_change_level_dense_ran), 2).alias("prev3_level_avg_3_prior"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "level_seq",
        "level_change",
        "prev3_level",
        "level_percent_rank",
        "level_dense_rank_3",
        "level_change_2",
        "team_level_avg_level",
        "level_sum_6_prior",
        "running_count",
        "running_level",
        "level_dense_rank_3_plus_level_dense_rank",
        "rows_in_team_level_distance",
        "level_dense_rank_tier",
        "next2_level_change_3",
        "prev3_level_avg_3_prior",
    )
    .orderBy(
        F.col("level_dense_rank_3").desc(),
        "level_dense_rank_3_plus_level_dense_rank",
        "id",
    )
    .limit(10)
)
