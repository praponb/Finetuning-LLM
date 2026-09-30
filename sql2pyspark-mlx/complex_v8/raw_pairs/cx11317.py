from pyspark.sql import functions as F

recent = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) | (F.col("track_id") >= 23))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.when(F.col("ms_played") >= 225733, "large").when(F.col("ms_played") >= 107912, "medium").otherwise("small").alias("ms_played_tier"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
    )
)

summarized_rows = (
    recent.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.last_played_at", "p.avg_play_ms_played")
)

listener_id_stats = (
    summarized_rows
    .groupBy("listener_id")
    .agg(F.avg("avg_play_ms_played").alias("avg_avg_play_ms_played"))
)

vs_group = (
    summarized_rows.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.avg_play_ms_played") > F.col("g.avg_avg_play_ms_played"))
    .select(
        "s.*",
        "g.avg_avg_play_ms_played",
        F.round(F.col("s.avg_play_ms_played") - F.col("g.avg_avg_play_ms_played"), 2).alias("avg_play_ms_played_minus_avg"),
    )
)

joined = (
    vs_group.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .filter(F.col("t.artist_id") <= 10)
    .select(
        "s.*",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

result = (
    joined
    .select("id", "ms_played", "track_bpm")
    .unpivot(
        ids=["id"],
        values=["ms_played", "track_bpm"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
