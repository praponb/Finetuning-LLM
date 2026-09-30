from pyspark.sql import functions as F

cleaned = (
    df_tracks
    .filter(F.col("bpm").between(76, 156))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.date_add(F.col("release_date"), 30).alias("release_date_plus_30d"),
        F.trunc(F.col("release_date"), "MM").alias("release_date_month_start"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

artist_id_stats = (
    cleaned
    .groupBy("artist_id")
    .agg(F.avg("bpm").alias("avg_bpm"))
)

above_average = (
    cleaned.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") > F.col("g.avg_bpm"))
    .select(
        "s.*",
        "g.avg_bpm",
        F.round(F.col("s.bpm") - F.col("g.avg_bpm"), 2).alias("bpm_minus_avg"),
    )
)

detail_rows = (
    above_average.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.bpm",
        "s.release_date_plus_30d",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

grouped = (
    detail_rows
    .groupBy("artist_id")
    .agg(
        F.max("release_date_plus_30d").alias("last_release_date_plus_30d"),
        F.percentile_approx("play_ms_played", 0.5).alias("median_play_ms_played"),
        F.sum(F.when(F.col("play_is_skipped"), F.col("duration_sec"))).alias("duration_sec_play_is_skipped_true"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.max("release_date").alias("last_track_release_date"),
    )
)

summarized_rows = (
    grouped.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.last_track_release_date",
    )
)

result = (
    summarized_rows
    .filter(F.col("median_play_ms_played") <= 261750)
    .select(
        "artist_id",
        "last_release_date_plus_30d",
        "median_play_ms_played",
        "duration_sec_play_is_skipped_true",
        "track_count",
        "last_track_release_date",
        (F.col("track_count") - F.col("median_play_ms_played")).alias("track_count_minus_median_play_ms_played"),
        F.greatest(F.col("median_play_ms_played"), F.col("track_count")).alias("greatest_median_play_ms_played_track_count"),
    )
    .orderBy(
        F.col("greatest_median_play_ms_played_track_count").desc(),
        "last_track_release_date",
        "artist_id",
    )
    .limit(5)
)
