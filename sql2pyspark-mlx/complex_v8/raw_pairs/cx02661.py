from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.col("duration_sec") <= 352))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

joined_artist = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", F.col("a.is_verified").alias("artist_is_verified"))
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.max("release_date").alias("last_track_release_date"),
        F.min("release_date").alias("first_track_release_date"),
        F.count("*").alias("track_count"),
    )
)

with_counts = (
    joined_artist.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        "t.last_track_release_date",
        "t.first_track_release_date",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

result = (
    with_counts
    .filter(F.col("bpm_bucket") > 96)
    .select(
        "id",
        "is_explicit",
        "bpm",
        "bpm_bucket",
        "title_slug",
        "artist_is_verified",
        "last_track_release_date",
        "track_count",
    )
    .orderBy("title_slug", "bpm")
)
