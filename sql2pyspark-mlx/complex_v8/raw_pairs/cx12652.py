from pyspark.sql import functions as F

filtered = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "sad"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
        F.greatest(F.col("duration_sec"), F.col("bpm")).alias("greatest_duration_sec_bpm"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

with_counts = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_played_at")
)

joined_artist = (
    with_counts.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.moods",
        "s.title_ref",
        "s.greatest_duration_sec_bpm",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
    )
)

line_level = (
    joined_artist.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.artist_genre",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

result = (
    line_level
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "artist_genre",
        "play_id",
        "play_played_at",
        "play_listener_id",
        (F.col("play_played_at") + F.expr("INTERVAL 15 MINUTES")).alias("play_played_at_plus_15m"),
    )
    .orderBy(
        F.col("artist_genre").desc(),
        F.col("play_played_at_plus_15m").desc_nulls_first(),
    )
)
