from pyspark.sql import functions as F

valid = df_plays.filter(F.col("played_at") < F.lit("2025-02-18 10:00:00").cast("timestamp"))

combined_details_track = (
    valid.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        F.col("t.release_date").alias("track_release_date"),
    )
)

combined_details_listener = (
    combined_details_track.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.birth_year") <= 1995), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.track_release_date",
        F.col("l.country").alias("listener_country"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.username").alias("listener_username"),
    )
)

enriched = (
    combined_details_listener
    .filter(F.col("track_id") >= 14)
    .select(
        "*",
        F.round(F.col("ms_played") / F.nullif(F.col("listener_birth_year"), F.lit(0)), 3).alias("ms_played_per_listener_birth_year"),
        F.round(F.col("listener_birth_year") / F.nullif(F.col("ms_played"), F.lit(0)), 3).alias("listener_birth_year_per_ms_played"),
    )
)

listener_id_stats = (
    enriched
    .groupBy("listener_id")
    .agg(F.percentile_approx("listener_birth_year", 0.5).alias("median_listener_birth_year"))
)

above_average = (
    enriched.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.listener_birth_year") >= F.col("g.median_listener_birth_year"))
    .select(
        "s.*",
        "g.median_listener_birth_year",
        F.round(F.col("s.listener_birth_year") - F.col("g.median_listener_birth_year"), 2).alias("listener_birth_year_minus_median"),
    )
)

result = (
    above_average
    .filter(F.col("median_listener_birth_year") >= 1974.4)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "track_release_date",
        "listener_birth_year",
        "listener_username",
        "ms_played_per_listener_birth_year",
        "listener_birth_year_per_ms_played",
        "listener_birth_year_minus_median",
    )
    .orderBy(
        F.col("listener_birth_year_minus_median").asc_nulls_last(),
        F.col("track_release_date").desc_nulls_first(),
    )
)
