from pyspark.sql import functions as F

cleaned_adjusters = (
    df_adjusters
    .filter(F.col("team") != "Travel")
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled_2"),
    )
)

tagged = (
    cleaned_adjusters
    .select(
        "*",
        F.coalesce(F.col("level_filled"), F.lit(0)).alias("level_filled_filled"),
        F.round(F.col("level_filled") / F.nullif(F.col("adjuster_name_len"), F.lit(0)), 3).alias("level_filled_per_adjuster_name_len"),
    )
)

team_stats = (
    tagged
    .groupBy("team")
    .agg(F.avg("level_filled_2").alias("avg_level_filled_2"))
)

vs_group = (
    tagged.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level_filled_2") > F.col("g.avg_level_filled_2"))
    .select(
        "s.*",
        "g.avg_level_filled_2",
        F.round(F.col("s.level_filled_2") - F.col("g.avg_level_filled_2"), 2).alias("level_filled_2_minus_avg"),
    )
)

result = (
    vs_group
    .rollup("level", "level_filled", "adjuster_name_len")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("team").alias("distinct_teams"),
        F.round(F.stddev("level_filled_filled"), 2).alias("stddev_level_filled_filled"),
    )
    .orderBy(
        F.col("level").asc_nulls_last(),
        F.col("level_filled").asc_nulls_last(),
        F.col("adjuster_name_len").asc_nulls_last(),
    )
)
