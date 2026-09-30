from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label_2"),
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count"))
)

summarized_rows = (
    filtered_tracks.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

result = (
    summarized_rows
    .cube("is_explicit_label_2", "track_count", "is_explicit_label")
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("n_rows"),
        F.sum("track_count").alias("total_track_count"),
    )
    .select(
        F.coalesce(F.col("is_explicit_label_2"), F.lit("ALL")).alias("is_explicit_label_2"),
        "track_count",
        F.coalesce(F.col("is_explicit_label"), F.lit("ALL")).alias("is_explicit_label"),
        "gid",
        "n_rows",
        "total_track_count",
    )
)
