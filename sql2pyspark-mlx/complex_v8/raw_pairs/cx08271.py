from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter((F.col("release_date") >= F.lit("2024-06-03").cast("date")) & (F.col("bpm").between(73, 143)))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "moods",
    )
)

exploded_detail_plays = (
    recent_tracks.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.ms_played").between(51883, 333795)), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.moods",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

with_activity = (
    exploded_detail_plays.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.play_listener_id")) & (F.col("l.birth_year") <= 1990), "left_semi")
)

result = (
    with_activity
    .rollup("is_explicit", "play_id")
    .agg(
        F.round(100 * F.count(F.when(~F.col("is_explicit"), 1)) / F.count("*"), 1).alias("pct_not_is_explicit"),
        F.count("*").alias("cnt"),
    )
)
