from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .filter((F.col("ms_played") <= 290754) & (F.col("played_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count"),
        F.min("played_at").alias("first_played_at"),
    )
)

profiled = (
    prepared_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.first_played_at",
    )
)

with_parent = (
    profiled.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.release_date") >= F.lit("2024-11-14").cast("date")), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        "s.last_played_at",
        "s.play_count",
        "s.first_played_at",
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

qualified = (
    with_parent
    .crossJoin(with_parent.agg(F.avg("track_duration_sec").alias("overall_avg_track_duration_sec")))
    .filter(F.col("track_duration_sec") > F.col("overall_avg_track_duration_sec"))
    .drop("overall_avg_track_duration_sec")
)

result = (
    qualified
    .select("id", "track_duration_sec", "play_count")
    .unpivot(
        ids=["id"],
        values=["track_duration_sec", "play_count"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
