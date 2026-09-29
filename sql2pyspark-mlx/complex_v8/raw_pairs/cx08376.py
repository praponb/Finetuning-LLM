from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_team = Window.partitionBy("level", "team")
w_all_by_level_prior_1 = Window.orderBy(F.col("level").desc(), "id").rowsBetween(-1, -1)
w_all_by_level = Window.orderBy(F.col("level").desc(), "id")
w_all_by_level_ties = Window.orderBy(F.col("level").desc())

recent = df_adjusters.filter(F.length(F.trim(F.col("adjuster_name"))) >= 5)

windowed = (
    recent
    .select(
        "id",
        "team",
        "level",
        F.round(F.col("level") - F.avg("level").over(w_level_team), 2).alias("level_vs_level_team_avg"),
        F.round(F.avg("level").over(w_all_by_level_prior_1), 2).alias("level_avg_1_prior"),
        F.lead("level").over(w_all_by_level).alias("next_level"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_level_team), F.lit(0)), 4).alias("level_share_of_level_team"),
        F.rank().over(w_all_by_level_ties).alias("level_rank"),
        F.ntile(3).over(w_all_by_level).alias("level_tercile"),
    )
)

team_stats = (
    windowed
    .groupBy("team")
    .agg(F.avg("level_tercile").alias("avg_level_tercile"))
)

above_average = (
    windowed.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level_tercile") > F.col("g.avg_level_tercile"))
    .select(
        "s.*",
        "g.avg_level_tercile",
        F.round(F.col("s.level_tercile") - F.col("g.avg_level_tercile"), 2).alias("level_tercile_minus_avg"),
    )
)

team_stats_2 = (
    above_average
    .groupBy("team")
    .agg(F.max("level_avg_1_prior").alias("max_level_avg_1_prior"))
)

compared = (
    above_average.alias("s")
    .join(team_stats_2.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level_avg_1_prior") == F.col("g.max_level_avg_1_prior"))
    .select(
        "s.*",
        "g.max_level_avg_1_prior",
        F.round(F.col("s.level_avg_1_prior") - F.col("g.max_level_avg_1_prior"), 2).alias("level_avg_1_prior_minus_max"),
    )
)

grouped = (
    compared
    .groupBy("level")
    .agg(
        F.array_sort(F.collect_set("team")).alias("team_list"),
        F.count(F.when(F.col("team") != "Health", 1)).alias("n_team_health"),
    )
)

result = (
    grouped
    .select(
        "level",
        "team_list",
        "n_team_health",
        F.coalesce(F.col("n_team_health"), F.lit(0)).alias("n_team_health_filled"),
        (F.floor(F.col("n_team_health") / 2) * 2).alias("n_team_health_bucket"),
    )
)
