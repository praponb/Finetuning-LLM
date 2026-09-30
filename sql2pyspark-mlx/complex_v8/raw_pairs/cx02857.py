from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
    )
)

joined_artist = (
    valid.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm",
        "s.moods",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_parent = (
    joined_artist.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.artist_is_verified",
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified_2"),
    )
)

line_level = (
    with_parent.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.device") != "desktop"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.moods",
        "s.artist_genre",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    line_level
    .select(
        "id",
        "artist_id",
        "release_date",
        "artist_genre",
        "play_id",
        "play_is_skipped",
        "play_source",
        "play_device",
        F.length(F.trim(F.col("play_device"))).alias("play_device_len"),
        F.size(F.col("moods")).alias("moods_count"),
    )
    .orderBy(
        F.col("play_device_len").desc_nulls_first(),
        F.col("artist_id").desc(),
        "id",
        "play_id",
    )
    .limit(10)
)
