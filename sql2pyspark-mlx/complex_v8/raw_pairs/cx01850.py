from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter((F.to_date(F.col("played_at")) < F.lit("2025-02-24").cast("date")) & (F.col("device").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
    )
)

listener_id_stats = (
    valid_plays
    .groupBy("listener_id")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

compared = (
    valid_plays.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

result = (
    compared
    .select("id", "avg_ms_played", "ms_played_minus_avg")
    .unpivot(
        ids=["id"],
        values=["avg_ms_played", "ms_played_minus_avg"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric_name")
)
