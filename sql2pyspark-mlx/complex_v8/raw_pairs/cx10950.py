from pyspark.sql import functions as F

scoped = (
    df_tracks
    .filter(F.col("bpm") > 82)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
    )
)

combined_details_artist = (
    scoped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        "a.artist_name",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
    )
)

result = (
    combined_details_artist
    .select("id", "bpm", "duration_sec")
    .unpivot(
        ids=["id"],
        values=["bpm", "duration_sec"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
