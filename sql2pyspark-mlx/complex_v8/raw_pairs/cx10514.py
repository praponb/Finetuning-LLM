from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter((F.col("track_id") >= 23) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("id").cast("string")).alias("source_ref"),
        F.concat_ws("|", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

combined_details_track = (
    recent_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select("s.*", F.col("t.is_explicit").alias("track_is_explicit"))
)

derived = (
    combined_details_track
    .select(
        "*",
        F.when(F.col("ms_played") >= 239955, "large").when(F.col("ms_played") >= 95960, "medium").otherwise("small").alias("ms_played_tier"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

track_id_stats = (
    derived
    .groupBy("track_id")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

vs_group = (
    derived.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

result = (
    vs_group
    .groupBy("listener_id", "ms_played_tier")
    .pivot("source", ["playlist", "radio", "album", "search"])
    .agg(F.avg("avg_ms_played"))
    .orderBy("listener_id", "ms_played_tier")
)
