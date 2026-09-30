from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter(F.col("ms_played") > 71476)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
    )
)

joined = (
    filtered_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.source_prefix",
        F.col("l.username").alias("listener_username"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

with_parent = (
    joined.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source_prefix",
        "s.listener_signup_date",
        "s.track_bpm",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.bpm").alias("track_bpm_2"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source_prefix",
        "track_bpm",
        "track_duration_sec",
        "track_moods",
        "track_bpm_2",
        "listener_birth_year",
        F.lower(F.regexp_replace(F.trim(F.col("source_prefix")), " ", "_")).alias("source_prefix_slug"),
    )
    .orderBy("id", "track_duration_sec")
)
