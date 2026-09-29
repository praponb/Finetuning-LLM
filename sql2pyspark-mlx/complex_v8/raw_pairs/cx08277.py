from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        F.greatest(F.col("bpm"), F.col("duration_sec")).alias("greatest_bpm_duration_sec"),
    )
)

with_activity = (
    recent_tracks.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id").isNotNull()) & (F.col("p.ms_played").between(112786, 245815)), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        F.col("bpm").alias("m_bpm"),
        F.col("duration_sec").alias("m_duration_sec"),
        F.col("greatest_bpm_duration_sec").alias("greatest_bpm_duration_sec_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_bpm", "m_duration_sec", "greatest_bpm_duration_sec_v"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
