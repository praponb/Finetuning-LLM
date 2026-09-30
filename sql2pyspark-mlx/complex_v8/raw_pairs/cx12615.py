from pyspark.sql import functions as F

prepared = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
        F.date_add(F.col("release_date"), 7).alias("release_date_plus_7d"),
    )
)

with_children = (
    prepared.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.device") == "speaker"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.release_date_year",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

combined_details = (
    with_children.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.release_date_year",
        "s.play_id",
        "s.play_device",
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped_2"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"), F.count("*").alias("play_count_2"))
)

with_stats = (
    combined_details.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

without_matches = (
    with_stats.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") > 85) & (F.col("t.release_date").between(F.lit("2024-06-03").cast("date"), F.lit("2025-11-29").cast("date"))), "left_anti")
)

grouped = (
    without_matches
    .groupBy("play_source")
    .agg(
        F.percentile_approx("play_count", 0.5).alias("median_play_count"),
        F.round(F.avg("play_count_2"), 2).alias("avg_play_count_2"),
        F.array_sort(F.collect_set("play_source")).alias("play_source_list"),
        F.max("play_count").alias("max_play_count_check"),
    )
    .filter(F.col("max_play_count_check") > 2)
    .drop("max_play_count_check")
)

without_matches_2 = (
    grouped
    .crossJoin(grouped.agg(F.avg("avg_play_count_2").alias("overall_avg_avg_play_count_2")))
    .filter(F.col("avg_play_count_2") > F.col("overall_avg_avg_play_count_2"))
    .drop("overall_avg_avg_play_count_2")
)

result = (
    without_matches_2
    .select(
        "play_source",
        "median_play_count",
        "play_source_list",
        F.array_join(F.array_sort(F.col("play_source_list")), ",").alias("play_source_list_text"),
    )
)
