from pyspark.sql import functions as F

recent_adjusters = (
    df_adjusters
    .filter(F.upper(F.trim(F.col("adjuster_name"))).like("G%"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.concat_ws("-", F.col("adjuster_name"), F.col("team")).alias("adjuster_name_team_label"),
        F.when(F.col("level") >= 3, "HIGH").when(F.col("level") >= 1, "MEDIUM").otherwise("LOW").alias("level_tier"),
    )
)

labeled = (
    recent_adjusters
    .select(
        "id",
        "adjuster_name",
        "level",
        "adjuster_name_team_label",
        "level_tier",
        F.lower(F.regexp_replace(F.trim(F.col("level_tier")), " ", "_")).alias("level_tier_slug"),
    )
)

level_tier_stats = (
    labeled
    .groupBy("level_tier")
    .agg(F.max("level").alias("max_level"))
)

compared = (
    labeled.alias("s")
    .join(level_tier_stats.alias("g"), F.col("g.level_tier") == F.col("s.level_tier"), "inner")
    .filter(F.col("s.level") == F.col("g.max_level"))
    .select(
        "s.*",
        "g.max_level",
        F.round(F.col("s.level") - F.col("g.max_level"), 2).alias("level_minus_max"),
    )
)

tagged = (
    compared
    .select(
        "id",
        "adjuster_name",
        "max_level",
        "level_minus_max",
        F.coalesce(F.col("level_minus_max"), F.lit(0.0)).alias("level_minus_max_filled"),
        (F.col("max_level") + F.col("level_minus_max")).alias("max_level_plus_level_minus_max"),
    )
)

result = (
    tagged
    .select(
        "id",
        "level_minus_max",
        "level_minus_max_filled",
        "max_level_plus_level_minus_max",
        "max_level",
    )
    .unpivot(
        ids=["id"],
        values=["level_minus_max", "level_minus_max_filled", "max_level_plus_level_minus_max", "max_level"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
