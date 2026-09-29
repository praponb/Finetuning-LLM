from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

joined = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.title_slug",
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.min("duration_sec").alias("min_track_duration_sec"),
        F.max("release_date").alias("last_track_release_date"),
        F.sum("bpm").alias("total_track_bpm"),
    )
)

with_counts = (
    joined.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        "t.min_track_duration_sec",
        "t.last_track_release_date",
        "t.total_track_bpm",
    )
)

result = (
    with_counts
    .groupBy("last_track_release_date")
    .pivot("artist_country", ["JP", "TH"])
    .agg(F.sum("total_track_bpm"))
    .orderBy("last_track_release_date")
)
