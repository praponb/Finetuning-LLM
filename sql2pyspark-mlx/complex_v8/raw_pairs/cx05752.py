from pyspark.sql import functions as F

scoped = (
    df_artists
    .filter(F.col("debut_year") >= 1997)
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.substring(F.trim(F.col("genre")), 1, 2).alias("genre_prefix"),
        F.when(F.col("debut_year") >= 2016, "HIGH").when(F.col("debut_year") >= 1996, "MEDIUM").otherwise("LOW").alias("debut_year_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("genre")), " ", "_")).alias("genre_slug"),
    )
)

line_level = (
    scoped.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_name",
        "s.country",
        "s.debut_year",
        F.col("t.id").alias("track_id"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

with_parent = (
    line_level.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.title").isNotNull())
    .select(
        "s.*",
        F.col("t.release_date").alias("track_release_date_2"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.sum("ms_played").alias("total_play_ms_played"),
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count"),
    )
)

profiled = (
    with_parent.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.total_play_ms_played",
        "p.last_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

result = (
    profiled
    .groupBy("track_release_date")
    .pivot("country", ["KR", "JP"])
    .agg(F.sum("play_count"))
    .orderBy("track_release_date")
)
