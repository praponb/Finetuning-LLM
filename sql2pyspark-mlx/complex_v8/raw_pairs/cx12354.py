from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter(F.col("ms_played") < 299017)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
    )
)

with_parent_listener = (
    prepared.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        F.col("l.username").alias("listener_username"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

listener_id_stats = (
    with_parent_listener
    .groupBy("listener_id")
    .agg(F.avg("track_duration_sec").alias("avg_track_duration_sec"))
)

vs_group = (
    with_parent_listener.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.track_duration_sec") > F.col("g.avg_track_duration_sec"))
    .select(
        "s.*",
        "g.avg_track_duration_sec",
        F.round(F.col("s.track_duration_sec") - F.col("g.avg_track_duration_sec"), 2).alias("track_duration_sec_minus_avg"),
    )
)

result = (
    vs_group
    .groupingSets(
        [["track_id", "device"], ["track_id"], ["device"]],
        "track_id", "device",
    )
    .agg(
        F.grouping("track_id").alias("is_total_track_id"),
        F.count("*").alias("cnt"),
        F.round(F.sum("track_duration_sec") / F.nullif(F.sum("track_bpm"), F.lit(0)), 3).alias("track_duration_sec_to_track_bpm_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("track_id").asc_nulls_last(), F.col("device").asc_nulls_last())
)
