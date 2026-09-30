from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter(F.col("duration_sec") <= 353)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

combined_details_artist = (
    valid.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

labeled = (
    combined_details_artist
    .filter(F.year(F.col("release_date")) == 2024)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "artist_genre",
        "artist_name",
        "artist_debut_year",
        (F.col("artist_debut_year") - F.col("duration_sec")).alias("artist_debut_year_minus_duration_sec"),
        F.date_add(F.col("release_date"), 7).alias("release_date_plus_7d"),
        F.concat_ws(": ", F.col("artist_genre"), F.col("artist_name")).alias("artist_genre_artist_name_label"),
    )
)

tagged = (
    labeled
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "artist_genre",
        "artist_name",
        "artist_debut_year",
        "artist_debut_year_minus_duration_sec",
        "release_date_plus_7d",
        "artist_genre_artist_name_label",
        F.concat_ws(" / ", F.col("artist_name"), F.col("artist_genre")).alias("artist_name_artist_genre_label"),
    )
)

artist_id_stats = (
    tagged
    .groupBy("artist_id")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

outliers = (
    tagged.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.duration_sec") == F.col("g.max_duration_sec"))
    .select(
        "s.*",
        "g.max_duration_sec",
        F.round(F.col("s.duration_sec") - F.col("g.max_duration_sec"), 2).alias("duration_sec_minus_max"),
    )
)

result = (
    outliers
    .filter(F.col("artist_debut_year") >= 1991)
    .select(
        "id",
        "artist_id",
        "release_date",
        "artist_genre",
        "artist_name",
        "artist_debut_year",
        "artist_debut_year_minus_duration_sec",
        "release_date_plus_7d",
        "artist_genre_artist_name_label",
        "artist_name_artist_genre_label",
        "duration_sec_minus_max",
        F.month(F.col("release_date")).alias("release_date_month"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date_plus_7d")).alias("days_since_release_date_plus_7d"),
    )
)
