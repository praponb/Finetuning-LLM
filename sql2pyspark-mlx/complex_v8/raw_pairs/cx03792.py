from pyspark.sql import functions as F

recent_adjusters = (
    df_adjusters
    .filter((F.lower(F.col("adjuster_name")).like("%su%")) & (F.col("level") < 3))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug"),
        F.substring(F.trim(F.col("adjuster_name")), 1, 3).alias("adjuster_name_prefix"),
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
    )
)

team_or_unknown_stats = (
    recent_adjusters
    .groupBy("team_or_unknown")
    .agg(F.max("level").alias("max_level"))
)

outliers = (
    recent_adjusters.alias("s")
    .join(team_or_unknown_stats.alias("g"), F.col("g.team_or_unknown") == F.col("s.team_or_unknown"), "inner")
    .filter(F.col("s.level") == F.col("g.max_level"))
    .select(
        "s.*",
        "g.max_level",
        F.round(F.col("s.level") - F.col("g.max_level"), 2).alias("level_minus_max"),
    )
)

annotated = (
    outliers
    .select(
        "id",
        "team",
        "level",
        "adjuster_name_prefix",
        "max_level",
        "level_minus_max",
        F.initcap(F.trim(F.col("team_slug"))).alias("team_slug_clean"),
        F.abs(F.col("level_minus_max") - 0.5).alias("level_minus_max_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug_2"),
    )
)

screened = (
    annotated.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.reported_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp")) & (F.col("c.claim_type") == "medical"), "left_semi")
)

result = (
    screened
    .select(
        "id",
        F.col("max_level").alias("max_level_v"),
        F.col("level_minus_max").alias("m_level_minus_max"),
    )
    .unpivot(
        ids=["id"],
        values=["max_level_v", "m_level_minus_max"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
