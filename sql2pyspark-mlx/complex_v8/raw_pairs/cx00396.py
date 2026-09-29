from pyspark.sql import functions as F

prepared = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

joined = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

combined_details_artist = (
    joined.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.genre").isin("Pop", "Classical")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        F.col("a.debut_year").alias("artist_debut_year_2"),
    )
)

without_matches = (
    combined_details_artist.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id").isNotNull()) & (F.col("p.ms_played") > 52009), "left_anti")
)

detail_rows_plays = (
    without_matches.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.source") == "album"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    detail_rows_plays
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        "play_id",
        "play_played_at",
        "play_ms_played",
        "play_source",
        "play_device",
        (F.col("play_played_at") + F.expr("INTERVAL 30 MINUTES")).alias("play_played_at_plus_30m"),
        F.greatest(F.col("bpm"), F.col("play_ms_played")).alias("greatest_bpm_play_ms_played"),
    )
    .orderBy(F.col("greatest_bpm_play_ms_played").desc())
)
