from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter(F.lower(F.col("title")).like("%on%"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

without_matches = (
    cleaned_tracks.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

enriched_artist = (
    without_matches.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", F.col("a.genre").alias("artist_genre"))
)

result = (
    enriched_artist
    .select("id", "duration_sec_filled", "bpm")
    .unpivot(
        ids=["id"],
        values=["duration_sec_filled", "bpm"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
