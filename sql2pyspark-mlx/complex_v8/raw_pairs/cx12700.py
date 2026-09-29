from pyspark.sql import functions as F

base = (
    df_adjusters
    .filter((F.upper(F.trim(F.col("adjuster_name"))).like("E%")) & (F.col("level") < 3))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.substring(F.trim(F.col("team")), 1, 4).alias("team_prefix"),
        F.concat(F.col("team"), F.lit("#"), F.col("level").cast("string")).alias("team_ref"),
    )
)

without_matches = (
    base
    .crossJoin(base.agg(F.avg("level").alias("overall_avg_level")))
    .filter(F.col("level") > F.col("overall_avg_level"))
    .drop("overall_avg_level")
)

tagged = (
    without_matches
    .filter(F.col("team_prefix").isNotNull())
    .select(
        "*",
        F.concat_ws("|", F.col("team"), F.col("team_ref")).alias("team_team_ref_label"),
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
        F.concat_ws(": ", F.col("team"), F.col("team_prefix")).alias("team_team_prefix_label"),
    )
)

tagged_2 = (
    tagged
    .select(
        "id",
        "adjuster_name",
        "team",
        "team_ref",
        "team_team_ref_label",
        F.concat_ws("-", F.col("team_team_prefix_label"), F.col("team_ref")).alias("team_team_prefix_label_team_ref_label"),
    )
)

result = (
    tagged_2
    .select(
        "id",
        "team",
        "team_ref",
        "team_team_ref_label",
        "team_team_prefix_label_team_ref_label",
        F.concat(F.col("team"), F.lit("#"), F.col("id").cast("string")).alias("team_ref_2"),
        F.concat_ws("-", F.col("team"), F.col("adjuster_name")).alias("team_adjuster_name_label"),
    )
    .orderBy(
        "team_ref_2",
        F.col("team_team_prefix_label_team_ref_label").desc_nulls_first(),
    )
)
