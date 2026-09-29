from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter(F.col("artist_id") <= 5)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.abs(F.col("duration_sec") - 184).alias("duration_sec_distance"),
    )
)

with_details_artist = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.debut_year") >= 1997)
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.duration_sec_distance",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

tagged = (
    with_details_artist
    .withColumn("duration_sec_distance_filled", F.coalesce(F.col("duration_sec_distance"), F.lit(0)))
)

result = (
    tagged
    .filter(F.col("artist_id") >= 7)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "duration_sec_distance",
        "artist_debut_year",
        "duration_sec_distance_filled",
    )
    .orderBy(F.col("id").desc())
)
