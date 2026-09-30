from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "focus")) & (F.col("bpm").between(76, 165)))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
    )
)

line_level = (
    prepared_tracks.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

screened = (
    line_level.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.play_listener_id")) & (F.col("l.birth_year").between(1966, 1995)), "left_anti")
)

tagged = (
    screened
    .filter(F.col("play_id").isNotNull())
    .select(
        "id",
        "play_id",
        "play_played_at",
        F.to_date(F.col("play_played_at")).alias("play_played_at_date"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.min("ms_played").alias("min_play_ms_played"),
        F.min("played_at").alias("first_played_at"),
    )
)

with_stats = (
    tagged.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        "p.min_play_ms_played",
        "p.first_played_at",
    )
)

rollup_base = (
    with_stats
    .filter(F.year(F.col("play_played_at_date")) == 2024)
    .groupBy(
        F.date_trunc("month", "play_played_at").alias("play_played_at_month"),
        F.date_format("play_played_at_date", "yyyy-MM").alias("play_played_at_date_ym"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("play_id").alias("distinct_play_ids"),
    )
)

result = (
    rollup_base
    .select("play_played_at_month", "row_count", "distinct_play_ids")
    .orderBy("row_count")
)
