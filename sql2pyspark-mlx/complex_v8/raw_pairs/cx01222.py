from pyspark.sql import functions as F

filtered_adjusters = (
    df_adjusters
    .filter(F.col("level") > 3)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.when(F.col("team").isin("Property", "Health"), "group_a").when(F.col("team") == "Travel", "group_b").otherwise("other").alias("team_group"),
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug"),
    )
)

with_activity = (
    filtered_adjusters
    .crossJoin(filtered_adjusters.agg(F.avg("level").alias("overall_avg_level")))
    .filter(F.col("level") > F.col("overall_avg_level"))
    .drop("overall_avg_level")
)

team_stats = (
    with_activity
    .groupBy("team")
    .agg(F.avg("level").alias("avg_level"))
)

compared = (
    with_activity.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level") > F.col("g.avg_level"))
    .select(
        "s.*",
        "g.avg_level",
        F.round(F.col("s.level") - F.col("g.avg_level"), 2).alias("level_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "team",
        "level",
        "team_group",
        "avg_level",
        "level_minus_avg",
        (F.col("level_minus_avg") - F.col("avg_level")).alias("level_minus_avg_minus_avg_level"),
    )
    .orderBy(F.col("level").desc())
)
