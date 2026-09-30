from pyspark.sql import functions as F

valid_artists = (
    df_artists
    .filter((F.col("country") == "KR") & (F.col("genre").isin("Indie", "Pop", "HipHop")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        F.lower(F.regexp_replace(F.trim(F.col("genre")), " ", "_")).alias("genre_slug"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.round(F.avg("bpm"), 2).alias("avg_track_bpm"),
        F.min("release_date").alias("first_track_release_date"),
        F.count("*").alias("track_count"),
    )
)

profiled = (
    valid_artists.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.avg_track_bpm",
        "t.first_track_release_date",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

track_stats_2 = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count_2"))
)

with_stats = (
    profiled.alias("s")
    .join(track_stats_2.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count_2"), F.lit(0)).alias("track_count_2"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "genre_slug",
        "avg_track_bpm",
        "first_track_release_date",
        "track_count_2",
        F.coalesce(F.col("avg_track_bpm"), F.lit(0.0)).alias("avg_track_bpm_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("genre_slug")), " ", "_")).alias("genre_slug_slug"),
    )
    .orderBy("first_track_release_date", "country", "id")
    .limit(5)
)
