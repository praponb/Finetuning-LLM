from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-02-18 12:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

source_stats = (
    valid_plays
    .groupBy("source")
    .agg(F.max("ms_played").alias("max_ms_played"))
)

outliers = (
    valid_plays.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played") == F.col("g.max_ms_played"))
    .select(
        "s.*",
        "g.max_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.max_ms_played"), 2).alias("ms_played_minus_max"),
    )
)

result = (
    outliers
    .select("id", "ms_played_minus_max", "max_ms_played")
    .unpivot(
        ids=["id"],
        values=["ms_played_minus_max", "max_ms_played"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric_name")
)
