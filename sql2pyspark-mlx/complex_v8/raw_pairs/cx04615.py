from pyspark.sql import functions as F

cleaned = (
    df_plays
    .filter((~F.col("is_skipped")) | (F.col("ms_played") < 239955))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.max("ms_played").alias("max_play_ms_played"))
)

with_counts = (
    cleaned.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.max_play_ms_played")
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

with_stats = (
    with_counts.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.total_play_ms_played",
    )
)

with_details = (
    with_stats.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (~F.col("t.is_explicit")), "inner")
    .filter(F.array_contains(F.col("t.moods"), "chill"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.max_play_ms_played",
        "s.play_count",
        "s.total_play_ms_played",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.moods").alias("track_moods"),
    )
)

result = (
    with_details
    .select(
        "id",
        "listener_id",
        "track_id",
        "max_play_ms_played",
        "play_count",
        "total_play_ms_played",
        "track_moods",
    )
    .orderBy(F.col("max_play_ms_played").asc_nulls_last())
)
