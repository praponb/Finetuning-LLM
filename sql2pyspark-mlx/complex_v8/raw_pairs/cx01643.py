from pyspark.sql import functions as F

base_artists = (
    df_artists
    .filter(F.upper(F.trim(F.col("artist_name"))).like("P%"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "is_verified",
        F.substring(F.trim(F.col("country")), 1, 3).alias("country_prefix"),
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
    )
)

detail_rows_tracks = (
    base_artists.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.country",
        "s.is_verified",
        "s.country_prefix",
        "s.debut_year_filled",
        F.col("t.id").alias("track_id"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.moods").alias("track_moods"),
    )
)

combined_details_track = (
    detail_rows_tracks.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.*",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.is_explicit").alias("track_is_explicit_2"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("a.country").alias("artist_country"),
    )
)

with_parent_artist = (
    combined_details_track.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.track_artist_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("t.release_date").alias("track_release_date_2"),
    )
)

exploded = (
    with_parent_artist
    .select(
        "id",
        "country",
        "is_verified",
        "country_prefix",
        "debut_year_filled",
        "track_id",
        "track_is_explicit",
        "track_duration_sec",
        "track_bpm",
        "track_release_date",
        "track_is_explicit_2",
        "track_artist_id",
        "artist_country",
        "artist_is_verified",
        "track_release_date_2",
        F.explode("track_moods").alias("track_mood"),
    )
)

result = (
    exploded
    .select(
        "id",
        "country",
        "is_verified",
        "country_prefix",
        "debut_year_filled",
        "track_id",
        "track_is_explicit",
        "track_duration_sec",
        "track_bpm",
        "track_is_explicit_2",
        "artist_is_verified",
        "track_release_date_2",
        "track_mood",
        F.add_months(F.col("track_release_date"), 3).alias("track_release_date_plus_3m"),
        F.datediff(F.col("track_release_date_2"), F.col("track_release_date")).alias("days_track_release_date_to_track_release_date_2"),
    )
)
