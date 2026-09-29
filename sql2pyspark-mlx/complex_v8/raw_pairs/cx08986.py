from pyspark.sql import functions as F

cleaned = (
    df_adjusters
    .filter((F.lower(F.col("adjuster_name")).like("%ha%")) & (F.col("level") < 3))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.substring(F.trim(F.col("adjuster_name")), 1, 4).alias("adjuster_name_prefix"),
        F.abs(F.col("level") - 3).alias("level_distance"),
        F.concat_ws("-", F.col("team"), F.col("adjuster_name")).alias("team_adjuster_name_label"),
    )
)

team_stats = (
    cleaned
    .groupBy("team")
    .agg(F.percentile_approx("level_distance", 0.5).alias("median_level_distance"))
)

compared = (
    cleaned.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level_distance") >= F.col("g.median_level_distance"))
    .select(
        "s.*",
        "g.median_level_distance",
        F.round(F.col("s.level_distance") - F.col("g.median_level_distance"), 2).alias("level_distance_minus_median"),
    )
)

annotated = (
    compared
    .withColumn("team_adjuster_name_label_len", F.length(F.trim(F.col("team_adjuster_name_label"))))
)

result = (
    annotated
    .cube("team_adjuster_name_label_len", "team", "level_distance")
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("team").alias("distinct_teams"),
    )
    .orderBy(
        F.col("team_adjuster_name_label_len").asc_nulls_last(),
        F.col("team").asc_nulls_last(),
        F.col("level_distance").asc_nulls_last(),
    )
)
