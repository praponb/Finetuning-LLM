from pyspark.sql import functions as F

recent_artists = df_artists.filter(F.col("debut_year") < 2015)

detail_rows = (
    recent_artists.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.debut_year",
        "s.is_verified",
        F.col("t.id").alias("track_id"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

result = (
    detail_rows
    .filter(F.col("is_verified"))
    .select(
        "id",
        "debut_year",
        "is_verified",
        "track_moods",
        "track_duration_sec",
        "track_release_date",
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("track_release_date")).alias("days_since_track_release_date"),
    )
    .orderBy(F.col("debut_year_filled").desc())
)
