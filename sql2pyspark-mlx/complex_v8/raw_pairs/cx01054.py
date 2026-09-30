from pyspark.sql import functions as F

valid = (
    df_plays
    .filter(F.col("device").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

joined_track = (
    valid.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.source",
        "s.source_prefix",
        "s.is_skipped_flag",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.title").alias("track_title"),
        F.col("a.genre").alias("artist_genre"),
    )
)

tagged = (
    joined_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source",
        "source_prefix",
        "is_skipped_flag",
        "track_is_explicit",
        "artist_genre",
        F.when(F.col("track_is_explicit"), "yes").otherwise("no").alias("track_is_explicit_label"),
    )
)

result = (
    tagged
    .groupingSets(
        [["track_is_explicit", "source", "track_is_explicit_label"], ["track_is_explicit"], ["source"], []],
        "track_is_explicit", "source", "track_is_explicit_label",
    )
    .agg(
        F.grouping("track_is_explicit").alias("is_total_track_is_explicit"),
        F.count("*").alias("cnt"),
        F.max("is_skipped_flag").alias("max_is_skipped_flag"),
    )
    .select(
        "track_is_explicit",
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        F.coalesce(F.col("track_is_explicit_label"), F.lit("ALL")).alias("track_is_explicit_label"),
        "is_total_track_is_explicit",
        "cnt",
        "max_is_skipped_flag",
    )
)
