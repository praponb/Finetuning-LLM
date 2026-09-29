from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_team_by_level_full = Window.partitionBy("level", "team").orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_team = Window.partitionBy("level", "team")
w_level_team_by_level = Window.partitionBy("level", "team").orderBy(F.col("level").desc(), "id")
w_level_by_level_running = Window.partitionBy("level").orderBy(F.col("level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_by_level_prior_6 = Window.partitionBy("level").orderBy(F.col("level").desc(), "id").rowsBetween(-6, -1)
w_level_by_level_running_2 = Window.partitionBy("level").orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_by_level_ties = Window.partitionBy("level").orderBy(F.col("level").desc())
w_level_by_level = Window.partitionBy("level").orderBy(F.col("level").desc(), "id")
w_level_by_level_ties_2 = Window.partitionBy("level").orderBy(F.col("level").asc_nulls_last())

prepared_adjusters = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

metrics = (
    prepared_adjusters
    .select(
        "*",
        F.first("level").over(w_level_team_by_level_full).alias("first_level"),
        (F.col("level") - F.lag("level").over(w_level_team_by_level)).alias("level_change"),
        F.last("level", ignorenulls=True).over(w_level_by_level_running).alias("last_known_level"),
        F.count(F.lit(1)).over(w_level_team).alias("rows_in_level_team"),
        F.round(F.avg("level").over(w_level_team), 2).alias("level_team_avg_level"),
        F.max("level").over(w_level_by_level_prior_6).alias("level_max_6_prior"),
        F.last("level", ignorenulls=True).over(w_level_by_level_running_2).alias("last_known_level_2"),
        F.dense_rank().over(w_level_by_level_ties).alias("level_dense_rank"),
        F.lead("level", 3).over(w_level_by_level).alias("next3_level"),
        F.dense_rank().over(w_level_by_level_ties_2).alias("level_dense_rank_2"),
        F.row_number().over(w_level_by_level).alias("rn"),
        F.min("level").over(w_level_by_level_running_2).alias("min_level_so_far"),
    )
)

derived = (
    metrics
    .select(
        "id",
        "level",
        "last_known_level",
        "rows_in_level_team",
        "level_max_6_prior",
        "last_known_level_2",
        "level_dense_rank",
        "next3_level",
        "level_dense_rank_2",
        "rn",
        (F.col("level_dense_rank") + F.col("rows_in_level_team")).alias("level_dense_rank_plus_rows_in_level_team"),
        F.when(F.col("rows_in_level_team") >= 13, "large").when(F.col("rows_in_level_team") >= 6, "medium").otherwise("small").alias("rows_in_level_team_tier"),
        F.substring(F.trim(F.col("team")), 1, 2).alias("team_prefix"),
    )
)

result = (
    derived
    .select(
        "id",
        "last_known_level",
        "rows_in_level_team",
        "level_max_6_prior",
        "last_known_level_2",
        "level_dense_rank",
        "next3_level",
        "level_dense_rank_2",
        "rn",
        "level_dense_rank_plus_rows_in_level_team",
        "rows_in_level_team_tier",
        "team_prefix",
        (F.col("level_max_6_prior") - F.col("rows_in_level_team")).alias("level_max_6_prior_minus_rows_in_level_team"),
        F.coalesce(F.col("last_known_level_2"), F.lit(0)).alias("last_known_level_2_filled"),
    )
)
