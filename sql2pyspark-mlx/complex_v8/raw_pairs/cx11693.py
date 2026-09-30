from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter((F.col("source").isin("radio", "playlist", "search")) & (F.col("ms_played") > 145720))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        (F.col("played_at") + F.expr("INTERVAL 7 DAYS")).alias("played_at_plus_7d"),
    )
)

with_parent_track = (
    scoped.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.ms_played_filled",
        "s.played_at_plus_7d",
        F.col("t.moods").alias("track_moods"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

track_id_stats = (
    with_parent_track
    .groupBy("track_id")
    .agg(F.max("track_duration_sec").alias("max_track_duration_sec"))
)

compared = (
    with_parent_track.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.track_duration_sec") == F.col("g.max_track_duration_sec"))
    .select(
        "s.*",
        "g.max_track_duration_sec",
        F.round(F.col("s.track_duration_sec") - F.col("g.max_track_duration_sec"), 2).alias("track_duration_sec_minus_max"),
    )
)

result = (
    compared
    .select(
        "id",
        "track_id",
        "source",
        "ms_played_filled",
        "track_moods",
        "track_duration_sec",
        "track_duration_sec_minus_max",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.round(F.col("max_track_duration_sec") * 0.9, 2).alias("max_track_duration_sec_adj"),
    )
    .orderBy(F.col("track_duration_sec_minus_max").asc())
)
