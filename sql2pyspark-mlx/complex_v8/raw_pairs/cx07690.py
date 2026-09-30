from pyspark.sql import functions as F

cleaned = (
    df_artists
    .filter(~F.col("genre").isin("HipHop", "Indie"))
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
        "is_verified",
    )
)

with_children_tracks = (
    cleaned.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.genre",
        "s.debut_year",
        F.col("t.id").alias("track_id"),
        F.col("t.title").alias("track_title"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

result = (
    with_children_tracks
    .groupBy("track_id", "track_release_date")
    .pivot("genre", ["Pop", "Indie", "HipHop", "Jazz"])
    .agg(F.min("debut_year"))
    .orderBy("track_id", "track_release_date")
)
