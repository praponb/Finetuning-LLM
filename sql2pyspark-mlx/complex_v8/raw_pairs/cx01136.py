from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.size(F.col("moods")) >= 2))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.add_months(F.col("release_date"), 6).alias("release_date_plus_6m"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
)

qualified = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre") == "Jazz") & (F.col("a.debut_year") >= 1996), "left_semi")
)

result = (
    qualified
    .rollup("is_explicit", "artist_id")
    .agg(F.max("duration_sec_filled").alias("max_duration_sec_filled"))
)
