from pyspark.sql import functions as F

cleaned = (
    df_artists
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
    )
)

qualified = (
    cleaned.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left_semi")
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
        F.min("duration_sec").alias("min_track_duration_sec"),
        F.sum("duration_sec").alias("total_track_duration_sec"),
    )
)

with_counts = (
    qualified.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
        "t.min_track_duration_sec",
        "t.total_track_duration_sec",
    )
)

result = (
    with_counts
    .rollup("genre", "track_is_explicit_count", "country")
    .agg(F.array_sort(F.collect_set("country")).alias("country_list"))
    .orderBy(
        F.col("genre").asc_nulls_last(),
        F.col("track_is_explicit_count").asc_nulls_last(),
        F.col("country").asc_nulls_last(),
    )
)
