from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter((F.col("track_id") <= 3) & (F.month(F.col("played_at")).isin(1, 3)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.when(F.col("device").isin("tv", "desktop"), "group_a").when(F.col("device") == "phone", "group_b").otherwise("other").alias("device_group"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
    )
)

enriched = (
    scoped_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.device_group",
        "s.days_since_played_at",
        "s.source_clean",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

with_children = (
    enriched.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.device_group",
        "s.days_since_played_at",
        "s.source_clean",
        "s.track_release_date",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

enriched_play = (
    with_children.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("p.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.device_group",
        "s.days_since_played_at",
        "s.source_clean",
        "s.track_release_date",
        "s.play_id",
        "s.play_is_skipped",
        "s.play_listener_id",
        "s.play_played_at",
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at_2"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.release_date").alias("track_release_date_2"),
    )
)

result = (
    enriched_play
    .groupingSets(
        [["play_track_id", "track_id"], ["play_track_id"], ["track_id"], []],
        "play_track_id", "track_id",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("play_device")).alias("play_device_list"),
        F.max("track_release_date_2").alias("last_track_release_date_2"),
    )
    .orderBy(
        F.col("play_track_id").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
    )
)
