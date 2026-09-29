from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter((F.col("track_id") >= 13) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.min("played_at").alias("first_played_at_2"),
        F.count(F.when(F.col("device") == "car", 1)).alias("play_device_count"),
    )
)

profiled = (
    cleaned_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        "p.first_played_at_2",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

with_details = (
    profiled.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.signup_date") < F.lit("2024-12-21").cast("date")), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.play_device_count",
        F.col("l.interests").alias("listener_interests"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.max("played_at").alias("last_played_at"),
    )
)

profiled_2 = (
    with_details.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.first_played_at", "p.last_played_at")
)

enriched_track = (
    profiled_2.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.col("t.moods").alias("track_moods"),
        F.col("t.title").alias("track_title"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

track_artist_id_stats = (
    enriched_track
    .groupBy("track_artist_id")
    .agg(F.avg("play_device_count").alias("avg_play_device_count"))
)

above_average = (
    enriched_track.alias("s")
    .join(track_artist_id_stats.alias("g"), F.col("g.track_artist_id") == F.col("s.track_artist_id"), "inner")
    .filter(F.col("s.play_device_count") > F.col("g.avg_play_device_count"))
    .select(
        "s.*",
        "g.avg_play_device_count",
        F.round(F.col("s.play_device_count") - F.col("g.avg_play_device_count"), 2).alias("play_device_count_minus_avg"),
    )
)

stats = (
    above_average
    .filter(F.col("play_device_count") > 2)
    .groupBy("track_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("played_at").alias("first_played_at_2"),
        F.sum(F.when(F.col("source").isNotNull(), 1).otherwise(0)).alias("n_source_is_not_null"),
        F.round(F.sum("ms_played") / F.nullif(F.sum("track_duration_sec"), F.lit(0)), 3).alias("ms_played_to_track_duration_sec_ratio"),
        F.sum(F.when(F.length(F.trim(F.col("track_title"))) >= 4, 1).otherwise(0)).alias("n_length_trim_track_title_4"),
        F.avg("play_device_count_minus_avg").alias("avg_play_device_count_minus_avg_check"),
    )
    .filter(F.col("avg_play_device_count_minus_avg_check") > -5.6)
    .drop("avg_play_device_count_minus_avg_check")
)

result = (
    stats
    .select(
        "track_id",
        "distinct_ids",
        "first_played_at_2",
        "n_source_is_not_null",
    )
)
