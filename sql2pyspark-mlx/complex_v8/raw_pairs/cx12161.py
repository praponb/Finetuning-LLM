from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_by_level = Window.partitionBy("level").orderBy(F.col("level").asc_nulls_last(), "id")
w_team_group = Window.partitionBy("team_group")
w_all_by_level_full = Window.orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_team_group_by_level_ti = Window.partitionBy("level", "team_group").orderBy(F.col("level").desc())
w_team_level = Window.partitionBy("team", "level")
w_level_team_group_by_level_pr = Window.partitionBy("level", "team_group").orderBy(F.col("level").desc(), "id").rowsBetween(-2, -1)

scoped = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.lower(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
        F.when(F.col("team").isin("Health", "Property"), "group_a").when(F.col("team") == "Travel", "group_b").otherwise("other").alias("team_group"),
    )
)

windowed = (
    scoped
    .select(
        "*",
        F.round(F.percent_rank().over(w_level_by_level), 3).alias("level_percent_rank"),
        F.round(F.col("level") - F.avg("level").over(w_team_group), 2).alias("level_vs_team_group_avg"),
        F.first("level").over(w_all_by_level_full).alias("first_level"),
        F.round(F.cume_dist().over(w_level_team_group_by_level_ti), 3).alias("level_cume_dist"),
        F.row_number().over(w_level_by_level).alias("rn"),
        F.count(F.lit(1)).over(w_team_level).alias("rows_in_team_level"),
        F.min("level").over(w_team_level).alias("team_level_min_level"),
        F.max("level").over(w_level_team_group_by_level_pr).alias("level_max_2_prior"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_team_level), F.lit(0)), 4).alias("level_share_of_team_level"),
    )
)

result = (
    windowed
    .filter(F.col("team_level_min_level").between(13, 33))
    .select(
        "id",
        "adjuster_name",
        "team",
        "adjuster_name_clean",
        "level_vs_team_group_avg",
        "first_level",
        "rn",
        "rows_in_team_level",
        "team_level_min_level",
        "level_max_2_prior",
        "level_share_of_team_level",
        F.coalesce(F.col("level_max_2_prior"), F.lit(0)).alias("level_max_2_prior_filled"),
    )
)
