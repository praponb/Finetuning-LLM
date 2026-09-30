from pyspark.sql import functions as F

recent = (
    df_plays
    .filter(F.col("device").isin("speaker", "car"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

with_stats = (
    recent.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

combined_details = (
    with_stats.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.length(F.trim(F.col("l.username"))) >= 6), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.ms_played_bucket",
        "s.device_len",
        "s.play_count",
        F.col("l.country").alias("listener_country"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

combined_details_2 = (
    combined_details.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.ms_played_bucket",
        "s.device_len",
        "s.listener_country",
        "s.listener_birth_year",
        "s.track_moods",
        "s.track_bpm",
        F.col("t.moods").alias("track_moods_2"),
    )
)

with_details = (
    combined_details_2.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.device_len",
        "s.listener_country",
        "s.listener_birth_year",
        "s.track_moods",
        "s.track_bpm",
        "s.track_moods_2",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.plan").alias("listener_plan"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

joined_track = (
    with_details.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
        F.col("l.plan").alias("listener_plan_2"),
        F.col("l.username").alias("listener_username"),
    )
)

with_parent = (
    joined_track.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        "s.listener_country",
        "s.listener_birth_year",
        "s.listener_signup_date",
        "s.track_is_explicit",
        "s.track_release_date",
        "s.track_duration_sec",
        "s.track_title",
        "s.listener_plan_2",
        "s.listener_username",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.country").alias("listener_country_2"),
        F.col("l.signup_date").alias("listener_signup_date_2"),
        F.col("t.release_date").alias("track_release_date_2"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        "listener_country",
        "listener_birth_year",
        "listener_signup_date",
        "track_is_explicit",
        "track_duration_sec",
        "track_title",
        "listener_plan_2",
        "listener_username",
        "listener_country_2",
        "listener_signup_date_2",
        "track_release_date_2",
        F.date_trunc("year", F.col("track_release_date_2")).alias("track_release_date_2_year"),
        F.coalesce(F.col("listener_country_2"), F.lit("UNKNOWN")).alias("listener_country_2_or_unknown"),
    )
    .orderBy(F.col("listener_signup_date").desc(), F.col("track_title").desc())
)
