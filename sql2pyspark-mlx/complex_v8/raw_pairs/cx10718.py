from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter((F.col("device").isNotNull()) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

with_details_track = (
    valid_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.played_at_label",
        "s.source_slug",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

exploded = (
    with_details_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "played_at_label",
        "source_slug",
        "track_is_explicit",
        "track_bpm",
        "listener_birth_year",
        F.explode_outer("listener_interests").alias("listener_interest"),
    )
)

enriched = (
    exploded.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .filter(F.col("l.birth_year") > 1975)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.played_at_label",
        "s.source_slug",
        "s.track_bpm",
        "s.listener_birth_year",
        "s.listener_interest",
        F.col("l.username").alias("listener_username"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.birth_year").alias("listener_birth_year_2"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

result = (
    enriched
    .rollup("listener_plan", "listener_id", "track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.array_sort(F.collect_set("listener_username")).alias("listener_username_list"),
    )
    .orderBy(
        F.col("listener_plan").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
    )
)
