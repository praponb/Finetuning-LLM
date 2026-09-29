from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter(~F.col("source").isin("album", "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.ms_played") < 302279), "left_semi")
    .filter(F.col("track_id").isNotNull())
)

with_parent_track = (
    without_matches.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.array_contains(F.col("t.moods"), "sad")), "left")
    .select(
        "s.*",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

combined_details = (
    with_parent_track.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.col("t.moods").alias("track_moods"),
        F.col("t.bpm").alias("track_bpm_2"),
    )
)

result = (
    combined_details
    .select(
        "id",
        F.col("track_bpm_2").alias("m_track_bpm_2"),
        F.col("track_bpm").alias("track_bpm_v"),
        F.col("track_duration_sec").alias("m_track_duration_sec"),
    )
    .unpivot(
        ids=["id"],
        values=["m_track_bpm_2", "track_bpm_v", "m_track_duration_sec"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
