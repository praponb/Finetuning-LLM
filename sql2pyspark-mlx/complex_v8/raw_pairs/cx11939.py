from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("M%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

joined = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_children_plays = (
    joined.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.artist_genre",
        "s.artist_is_verified",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

with_details = (
    with_children_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.id") == F.col("s.play_id")) & (F.col("p.device").isNotNull()), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("p.track_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.artist_is_verified",
        "s.play_id",
        "s.play_source",
        "s.play_listener_id",
        "s.play_played_at",
        F.col("p.device").alias("play_device_2"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    with_details
    .rollup("play_id", "play_source", "play_device_2")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.min("play_played_at").alias("first_play_played_at"),
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("title"))).like("C%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_title_like_c"),
    )
    .orderBy(
        F.col("play_id").asc_nulls_last(),
        F.col("play_source").asc_nulls_last(),
        F.col("play_device_2").asc_nulls_last(),
    )
)
