from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 2) & (F.lower(F.col("title")).like("%w %")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
    )
)

annotated = (
    cleaned_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "*",
        F.abs(F.col("duration_sec") - 279).alias("duration_sec_distance"),
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
    )
)

flattened = (
    annotated
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "duration_sec_distance",
        "release_date_year",
        F.explode("moods").alias("mood"),
    )
)

with_details = (
    flattened.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.release_date_year",
        "s.mood",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

joined_artist = (
    with_details.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.release_date_year",
        "s.mood",
        "s.artist_debut_year",
        "a.artist_name",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

result = (
    joined_artist
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "release_date_year",
        "mood",
        "artist_name",
        F.initcap(F.trim(F.col("artist_name"))).alias("artist_name_clean"),
    )
    .orderBy(F.col("artist_id").desc(), "id", "mood")
    .limit(5)
)
