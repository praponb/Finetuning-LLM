from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
    )
)

with_activity = (
    valid_tracks.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left_semi")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("device").isin("speaker", "tv", "car"), 1)).alias("play_device_count"),
    )
)

summarized_rows = (
    with_activity.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

result = (
    summarized_rows
    .filter(F.col("play_device_count") >= 2)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "play_device_count",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
    .orderBy(F.col("release_date").desc())
)
