from pyspark.sql import functions as F

prepared_adjusters = (
    df_adjusters
    .filter(F.col("level") < 3)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.upper(F.trim(F.col("team"))).alias("team_clean"),
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        F.upper(F.trim(F.col("team"))).alias("team_clean_2"),
    )
)

annotated = (
    prepared_adjusters
    .select(
        "*",
        F.lower(F.trim(F.col("team_clean_2"))).alias("team_clean_2_clean"),
        F.concat_ws(": ", F.col("adjuster_name"), F.col("team_clean")).alias("adjuster_name_team_clean_label"),
        F.concat_ws("-", F.col("team_clean"), F.col("team_or_unknown")).alias("team_clean_team_or_unknown_label"),
    )
)

without_matches = (
    annotated.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.claim_type").isin("medical", "flood")), "left_semi")
)

tagged = (
    without_matches
    .filter(F.col("team_or_unknown") == "Property")
    .select(
        "id",
        "team",
        "level",
        "team_clean",
        "team_or_unknown",
        "team_clean_2",
        "team_clean_2_clean",
        "adjuster_name_team_clean_label",
        "team_clean_team_or_unknown_label",
        F.lower(F.regexp_replace(F.trim(F.col("team_or_unknown")), " ", "_")).alias("team_or_unknown_slug"),
        F.when(F.col("team_or_unknown").isin("Health", "Motor"), "group_a").when(F.col("team_or_unknown") == "UNKNOWN", "group_b").otherwise("other").alias("team_or_unknown_group"),
    )
)

annotated_2 = (
    tagged
    .select(
        "id",
        "team",
        "level",
        "team_or_unknown",
        "team_clean_2",
        "team_clean_2_clean",
        "team_clean_team_or_unknown_label",
        "team_or_unknown_slug",
        "team_or_unknown_group",
        F.when(F.col("level") >= 3, "large").when(F.col("level") >= 2, "medium").otherwise("small").alias("level_tier"),
    )
)

team_stats = (
    annotated_2
    .groupBy("team")
    .agg(F.max("level").alias("max_level"))
)

outliers = (
    annotated_2.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level") == F.col("g.max_level"))
    .select(
        "s.*",
        "g.max_level",
        F.round(F.col("s.level") - F.col("g.max_level"), 2).alias("level_minus_max"),
    )
)

result = (
    outliers
    .select(
        "id",
        "team",
        "level",
        "team_or_unknown",
        "team_clean_2",
        "team_clean_team_or_unknown_label",
        "team_or_unknown_slug",
        "team_or_unknown_group",
        "level_tier",
        "max_level",
        "level_minus_max",
        F.round(F.col("level_minus_max") * 1.1, 2).alias("level_minus_max_adj"),
    )
    .orderBy("team_or_unknown")
)
