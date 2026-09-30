from pyspark.sql import functions as F

cleaned_tracks = df_tracks.filter(~F.col("is_explicit"))

with_details_artist = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.debut_year") < 2018)
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

enriched = (
    with_details_artist
    .select(
        "*",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

enriched_artist = (
    enriched.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.artist_debut_year",
        "s.title_prefix",
        "s.days_since_release_date",
        "s.title_slug",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year_2"),
        "a.artist_name",
    )
)

tagged = (
    enriched_artist
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "artist_debut_year",
        "title_prefix",
        "days_since_release_date",
        "artist_genre",
        "artist_debut_year_2",
        F.concat_ws("|", F.col("title"), F.col("title_slug")).alias("title_title_slug_label"),
        F.date_add(F.col("release_date"), 365).alias("release_date_plus_365d"),
    )
)

enriched_2 = (
    tagged
    .select(
        "*",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
        F.round(F.col("artist_debut_year") / F.nullif(F.col("artist_debut_year_2"), F.lit(0)), 3).alias("artist_debut_year_per_artist_debut_year_2"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "title",
        "release_date",
        "artist_debut_year",
        "title_prefix",
        "days_since_release_date",
        "artist_genre",
        "title_title_slug_label",
        "release_date_month",
        "artist_debut_year_per_artist_debut_year_2",
        "title_slug",
    )
)
