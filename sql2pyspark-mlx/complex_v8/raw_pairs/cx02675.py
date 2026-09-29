from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter(~F.array_contains(F.col("moods"), "workout"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

summarized_rows = (
    recent_tracks.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

enriched_artist = (
    summarized_rows.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre") == "Jazz")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.days_since_release_date",
        "s.release_date_dayofweek",
        "s.title_ref",
        "s.play_is_skipped_count",
        F.col("a.genre").alias("artist_genre"),
    )
)

exploded_detail = (
    enriched_artist.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.device").isin("tv", "desktop", "phone")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.release_date_dayofweek",
        "s.title_ref",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

qualified = (
    exploded_detail.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.array_contains(F.col("t.moods"), "party")) & (F.col("t.duration_sec") > 183), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "release_date_dayofweek",
        "title_ref",
        "play_id",
        "play_played_at",
        F.substring(F.trim(F.col("title_ref")), 1, 4).alias("title_ref_prefix"),
        F.add_months(F.col("release_date"), 6).alias("release_date_plus_6m"),
    )
)
