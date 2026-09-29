from pyspark.sql import functions as F

valid_artists = (
    df_artists
    .filter(F.length(F.trim(F.col("artist_name"))) >= 4)
    .select(
        "id",
        "artist_name",
        "genre",
        "is_verified",
    )
)

annotated = (
    valid_artists
    .filter(F.col("is_verified"))
    .select(
        "id",
        "artist_name",
        "genre",
        F.when(F.col("genre").isin("Classical", "HipHop"), "group_a").when(F.col("genre") == "Pop", "group_b").otherwise("other").alias("genre_group"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count"))
)

with_stats = (
    annotated.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

result = (
    with_stats
    .select("id", "genre", "genre_group")
    .orderBy("id", "genre")
)
