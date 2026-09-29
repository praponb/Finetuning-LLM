from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter(F.lower(F.col("title")).like("%ne%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
    )
)

enriched = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.title_len",
        "s.title_slug",
        "s.bpm_minus_duration_sec",
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
    )
)

tagged = (
    enriched
    .select(
        "id",
        "artist_id",
        "release_date",
        "title_slug",
        "bpm_minus_duration_sec",
        "artist_is_verified",
        "artist_name",
        F.coalesce(F.col("title_len"), F.lit(0)).alias("title_len_filled"),
    )
)

without_matches = (
    tagged.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.source").isNotNull()) & (F.col("p.listener_id") <= 2), "left_anti")
)

artist_id_stats = (
    without_matches
    .groupBy("artist_id")
    .agg(F.avg("title_len_filled").alias("avg_title_len_filled"))
)

above_average = (
    without_matches.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.title_len_filled") > F.col("g.avg_title_len_filled"))
    .select(
        "s.*",
        "g.avg_title_len_filled",
        F.round(F.col("s.title_len_filled") - F.col("g.avg_title_len_filled"), 2).alias("title_len_filled_minus_avg"),
    )
)

detail_rows = (
    above_average.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm_minus_duration_sec",
        "s.title_len_filled",
        "s.title_len_filled_minus_avg",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

combined_details_listener = (
    detail_rows.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.play_listener_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm_minus_duration_sec",
        "s.title_len_filled",
        "s.title_len_filled_minus_avg",
        "s.play_id",
        "s.play_played_at",
        "s.play_listener_id",
        F.col("l.country").alias("listener_country"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

result = (
    combined_details_listener
    .filter(F.col("play_listener_id") <= 4)
    .select(
        "id",
        "artist_id",
        "bpm_minus_duration_sec",
        "title_len_filled",
        "title_len_filled_minus_avg",
        "play_id",
        "play_played_at",
        "listener_country",
        "listener_birth_year",
        F.abs(F.col("bpm_minus_duration_sec") - 411).alias("bpm_minus_duration_sec_distance"),
        F.length(F.trim(F.col("listener_country"))).alias("listener_country_len"),
    )
    .orderBy(F.col("artist_id").desc())
)
