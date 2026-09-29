from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_by_avg_level = Window.partitionBy("team").orderBy(F.col("avg_level").asc_nulls_last(), "id")
w_team_group_team = Window.partitionBy("team_group", "team")
w_team_by_avg_level_ties = Window.partitionBy("team").orderBy(F.col("avg_level").asc_nulls_last())
w_level_by_level = Window.partitionBy("level").orderBy("level", "id")
w_level = Window.partitionBy("level")
w_level_by_level_ties = Window.partitionBy("level").orderBy("level")

scoped = (
    df_adjusters
    .filter((F.col("team") == "Property") & (F.upper(F.trim(F.col("adjuster_name"))).like("G%")))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.initcap(F.trim(F.col("team"))).alias("team_clean"),
        F.lower(F.trim(F.col("team"))).alias("team_clean_2"),
        F.when(F.col("team").isin("Motor", "Travel"), "group_a").when(F.col("team") == "Health", "group_b").otherwise("other").alias("team_group"),
    )
)

team_stats = (
    scoped
    .groupBy("team")
    .agg(F.avg("level").alias("avg_level"))
)

vs_group = (
    scoped.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level") > F.col("g.avg_level"))
    .select(
        "s.*",
        "g.avg_level",
        F.round(F.col("s.level") - F.col("g.avg_level"), 2).alias("level_minus_avg"),
    )
)

with_trends = (
    vs_group
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "team_group",
        "level_minus_avg",
        F.dense_rank().over(w_team_by_avg_level).alias("avg_level_dense_rank"),
        F.sum("avg_level").over(w_team_group_team).alias("team_group_team_sum_avg_level"),
        F.round(F.col("avg_level") / F.nullif(F.sum("avg_level").over(w_team_group_team), F.lit(0)), 4).alias("avg_level_share_of_team_group_team"),
        F.rank().over(w_team_by_avg_level_ties).alias("avg_level_rank"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.dense_rank().over(w_level_by_level).alias("level_dense_rank"),
        F.round(F.avg("avg_level_dense_rank").over(w_level), 2).alias("level_avg_avg_level_dense_rank"),
        F.round(F.cume_dist().over(w_level_by_level_ties), 3).alias("level_cume_dist"),
        (F.col("level_minus_avg") - F.lag("level_minus_avg").over(w_level_by_level)).alias("level_minus_avg_change"),
        (F.col("level") - F.lag("level").over(w_level_by_level)).alias("level_change"),
    )
)

result = (
    ranked
    .groupingSets(
        [["level_dense_rank", "team_group", "level"], ["level_dense_rank"], []],
        "level_dense_rank", "team_group", "level",
    )
    .agg(
        F.sum(F.when(F.length(F.trim(F.col("adjuster_name"))) >= 7, F.col("avg_level_rank")).otherwise(0)).alias("avg_level_rank_length_trim_adjuster_name_7"),
        F.count("*").alias("row_count"),
    )
)
