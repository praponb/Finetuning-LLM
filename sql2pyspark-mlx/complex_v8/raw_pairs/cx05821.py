from pyspark.sql import functions as F

base = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "sad")) | (F.col("bpm") >= 76))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "moods",
    )
)

with_activity = (
    base.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.duration_sec") > 165), "left_anti")
    .filter(F.array_contains(F.col("moods"), "chill"))
)

tagged = (
    with_activity
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count"))
)

with_counts = (
    tagged.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.min("ms_played").alias("min_play_ms_played"),
        F.count("*").alias("play_count"),
    )
)

with_counts_2 = (
    with_counts.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        "p.min_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

enriched = (
    with_counts_2
    .select(
        "*",
        F.length(F.trim(F.col("title_prefix"))).alias("title_prefix_len"),
        F.array_contains(F.col("moods"), "sad").alias("has_sad"),
    )
)

artist_id_stats = (
    enriched
    .groupBy("artist_id")
    .agg(F.avg("days_since_release_date").alias("avg_days_since_release_date"))
)

compared = (
    enriched.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.days_since_release_date") > F.col("g.avg_days_since_release_date"))
    .select(
        "s.*",
        "g.avg_days_since_release_date",
        F.round(F.col("s.days_since_release_date") - F.col("g.avg_days_since_release_date"), 2).alias("days_since_release_date_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "artist_id",
        "moods",
        "days_since_release_date",
        "title_prefix",
        "track_count",
        "min_play_ms_played",
        "has_sad",
        "avg_days_since_release_date",
        "days_since_release_date_minus_avg",
        F.date_add(F.col("release_date"), 365).alias("release_date_plus_365d"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
    .orderBy("days_since_release_date_minus_avg", "id")
    .limit(5)
)
