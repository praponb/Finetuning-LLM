from pyspark.sql import functions as F

recent_artists = (
    df_artists
    .filter((F.col("artist_name").isNotNull()) | (F.col("genre") == "HipHop"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        F.concat(F.col("genre"), F.lit("#"), F.col("id").cast("string")).alias("genre_ref"),
    )
)

detail_rows = (
    recent_artists.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_name",
        "s.country",
        F.col("t.id").alias("track_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.moods").alias("track_moods"),
    )
)

exploded = (
    detail_rows
    .select(
        "id",
        "artist_name",
        "country",
        "track_id",
        "track_duration_sec",
        "track_title",
        "track_release_date",
        F.explode_outer("track_moods").alias("track_mood"),
    )
)

screened = (
    exploded
    .crossJoin(exploded.agg(F.max("track_duration_sec").alias("overall_max_track_duration_sec")))
    .filter(F.col("track_duration_sec") < F.col("overall_max_track_duration_sec"))
    .drop("overall_max_track_duration_sec")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("device") == "desktop", 1)).alias("play_device_count"),
    )
)

with_stats = (
    screened.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "artist_name",
        "track_id",
        "track_duration_sec",
        "track_title",
        "track_release_date",
        "track_mood",
        "play_device_count",
        F.concat_ws(" / ", F.col("track_mood"), F.col("track_title")).alias("track_mood_track_title_label"),
    )
    .orderBy("play_device_count")
)
