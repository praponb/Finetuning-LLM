from pyspark.sql import functions as F

base = (
    df_plays
    .filter(F.col("device").isin("tv", "phone"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.when(F.col("ms_played") >= 223262, "HIGH").when(F.col("ms_played") >= 168782, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

joined_track = (
    base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.source",
        "s.ms_played_tier",
        "s.is_skipped_label",
        F.col("t.moods").alias("track_moods"),
        F.col("l.interests").alias("listener_interests"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("ms_played").alias("min_play_ms_played"),
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    joined_track.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.min_play_ms_played",
        "p.last_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

without_matches = (
    summarized_rows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (~F.col("p.is_skipped")), "left_anti")
)

result = (
    without_matches
    .cube("listener_id", "is_skipped_label", "play_count")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.avg("ms_played"), 2).alias("avg_ms_played"),
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("is_skipped_label").asc_nulls_last(),
        F.col("play_count").asc_nulls_last(),
    )
)
