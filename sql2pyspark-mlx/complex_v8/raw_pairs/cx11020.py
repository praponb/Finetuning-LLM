from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter(F.col("listener_id") <= 11)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        (F.col("played_at") + F.expr("INTERVAL 15 MINUTES")).alias("played_at_plus_15m"),
    )
)

joined = (
    cleaned_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.source_slug",
        "s.source_or_unknown",
        F.col("t.moods").alias("track_moods"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
    )
)

enriched = (
    joined
    .filter(F.col("artist_debut_year") <= 2016)
    .select(
        "*",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
    )
)

listener_id_stats = (
    enriched
    .groupBy("listener_id")
    .agg(F.percentile_approx("ms_played", 0.5).alias("median_ms_played"))
)

compared = (
    enriched.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.ms_played") >= F.col("g.median_ms_played"))
    .select(
        "s.*",
        "g.median_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.median_ms_played"), 2).alias("ms_played_minus_median"),
    )
)

flattened = (
    compared
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        "source_slug",
        "source_or_unknown",
        "artist_debut_year",
        "artist_genre",
        "played_at_label",
        "median_ms_played",
        "ms_played_minus_median",
        F.explode_outer("track_moods").alias("track_mood"),
    )
)

joined_2 = (
    flattened.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.source",
        "s.source_slug",
        "s.artist_debut_year",
        "s.played_at_label",
        "s.median_ms_played",
        "s.ms_played_minus_median",
        "s.track_mood",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("l.username").alias("listener_username"),
    )
)

result = (
    joined_2
    .select(
        "id",
        "track_id",
        "source",
        "source_slug",
        "artist_debut_year",
        "played_at_label",
        "median_ms_played",
        "track_mood",
        "track_is_explicit",
        "track_bpm",
        "listener_username",
        F.round(F.col("ms_played_minus_median") * 0.25, 2).alias("ms_played_minus_median_adj"),
        F.when(F.col("source").isin("search", "playlist"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
    )
    .orderBy(F.col("played_at_label").desc(), "source_slug")
)
