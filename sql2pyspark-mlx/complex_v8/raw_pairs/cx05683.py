from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter((F.col("source") != "playlist") & (F.col("played_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.length(F.trim(F.col("device"))).alias("device_len_2"),
    )
)

enriched_track = (
    recent_plays.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (~F.col("t.is_explicit")), "inner")
    .filter(F.col("t.is_explicit"))
    .select("s.*", F.col("t.is_explicit").alias("track_is_explicit"))
)

result = (
    enriched_track
    .select("id", "device_len", "ms_played")
    .unpivot(
        ids=["id"],
        values=["device_len", "ms_played"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric")
)
