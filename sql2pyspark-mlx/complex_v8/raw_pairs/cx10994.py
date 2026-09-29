from pyspark.sql import functions as F

recent = (
    df_plays
    .filter((F.col("ms_played") < 308104) & (F.col("track_id") >= 14))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
    )
)

annotated = (
    recent
    .withColumn("device_group", F.when(F.col("device").isin("speaker", "phone"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other"))
)

detail_rows_plays = (
    annotated.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.played_at_label",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

enriched_track = (
    detail_rows_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.duration_sec") >= 209)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.played_at_label",
        "s.play_id",
        "s.play_played_at",
        "s.play_listener_id",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

result = (
    enriched_track
    .select(
        "id",
        "play_id",
        "track_duration_sec",
        "track_bpm",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["track_duration_sec", "track_bpm"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
