from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter((F.col("track_id") <= 25) & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
    )
)

joined = (
    filtered_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        F.col("t.moods").alias("track_moods"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

scored = (
    joined
    .select(
        "*",
        F.array_contains(F.col("track_moods"), "focus").alias("has_focus"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        F.when(F.col("artist_is_verified"), "yes").otherwise("no").alias("artist_is_verified_label"),
    )
)

result = (
    scored
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "track_moods",
        "track_artist_id",
        "artist_country",
        "has_focus",
        "source_or_unknown",
        "artist_is_verified_label",
        F.concat(F.col("artist_country"), F.lit("#"), F.col("track_id").cast("string")).alias("artist_country_ref"),
        F.upper(F.trim(F.col("artist_country"))).alias("artist_country_clean"),
    )
    .orderBy(F.col("artist_country").desc(), F.col("source_or_unknown").desc())
)
