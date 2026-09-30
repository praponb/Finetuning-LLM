from pyspark.sql import functions as F

recent_artists = (
    df_artists
    .filter((~F.col("is_verified")) | (F.col("debut_year") <= 2018))
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        F.coalesce(F.col("genre"), F.lit("UNKNOWN")).alias("genre_or_unknown"),
    )
)

line_level = (
    recent_artists.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (~F.col("t.is_explicit")), "inner")
    .select(
        "s.id",
        "s.artist_name",
        "s.country",
        "s.genre_or_unknown",
        F.col("t.id").alias("track_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

enriched = (
    line_level.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.artist_name",
        "s.country",
        "s.track_id",
        "s.track_duration_sec",
        "s.track_is_explicit",
        "s.track_release_date",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.duration_sec").alias("track_duration_sec_2"),
        F.col("t.release_date").alias("track_release_date_2"),
    )
)

without_matches = (
    enriched.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.track_artist_id")) & (F.size(F.col("t.moods")) >= 2), "left_semi")
)

result = (
    without_matches
    .filter(F.col("track_duration_sec") > 144)
    .select(
        "id",
        "artist_name",
        "country",
        "track_id",
        "track_is_explicit",
        "track_release_date",
        "track_artist_id",
        F.date_trunc("month", F.col("track_release_date")).alias("track_release_date_month"),
    )
)
