from pyspark.sql import functions as F

filtered = (
    df_plays
    .filter(F.col("source").isin("search", "album"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.concat(F.col("device"), F.lit("#"), F.col("listener_id").cast("string")).alias("device_ref"),
        F.date_trunc("hour", F.col("played_at")).alias("played_at_hour"),
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
    )
)

with_activity = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.size(F.col("l.interests")) >= 2), "left_semi")
)

labeled = (
    with_activity
    .select(
        "id",
        "track_id",
        "source",
        "device_ref",
        "source_clean",
        F.round((F.unix_timestamp(F.col("played_at_hour")) - F.unix_timestamp(F.col("played_at"))) / 60, 1).alias("minutes_played_at_to_played_at_hour"),
        F.round((F.unix_timestamp(F.col("played_at_hour")) - F.unix_timestamp(F.col("played_at"))) / 3600, 1).alias("hours_played_at_to_played_at_hour"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

annotated = (
    labeled
    .withColumn("source_clean_prefix", F.substring(F.trim(F.col("source_clean")), 1, 4))
)

enriched = (
    annotated.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.is_explicit")), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.track_id",
        "s.device_ref",
        "s.source_clean",
        "s.minutes_played_at_to_played_at_hour",
        "s.hours_played_at_to_played_at_hour",
        "s.source_slug",
        "s.source_clean_prefix",
        F.col("t.moods").alias("track_moods"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

line_level_plays = (
    enriched.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.track_id",
        "s.source_slug",
        "s.track_moods",
        "s.track_release_date",
        "s.artist_is_verified",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
    )
)

result = (
    line_level_plays
    .filter(F.col("play_device").isNotNull())
    .select(
        "id",
        "track_moods",
        "track_release_date",
        "artist_is_verified",
        "play_id",
        F.coalesce(F.col("play_device"), F.lit("UNKNOWN")).alias("play_device_or_unknown"),
    )
    .orderBy(F.col("play_device_or_unknown").desc(), F.col("play_id").asc())
)
