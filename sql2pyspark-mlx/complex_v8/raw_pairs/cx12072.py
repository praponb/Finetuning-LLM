from pyspark.sql import functions as F

prepared_artists = (
    df_artists
    .filter((~F.col("is_verified")) & (F.col("artist_name").isNotNull()))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count"), F.count("*").alias("track_count_2"))
)

profiled = (
    prepared_artists.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        F.coalesce(F.col("t.track_count_2"), F.lit(0)).alias("track_count_2"),
    )
)

with_activity = (
    profiled
    .crossJoin(profiled.agg(F.avg("track_count").alias("overall_avg_track_count")))
    .filter(F.col("track_count") > F.col("overall_avg_track_count"))
    .drop("overall_avg_track_count")
)

result = (
    with_activity
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        "track_count",
    )
    .orderBy("genre", F.col("track_count").asc())
)
