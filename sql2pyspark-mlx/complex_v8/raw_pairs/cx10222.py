from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter(F.col("duration_sec") <= 352)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        (F.col("duration_sec") + F.col("bpm")).alias("duration_sec_plus_bpm"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.min("release_date").alias("first_track_release_date"))
)

profiled = (
    cleaned_tracks.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.first_track_release_date")
)

result = (
    profiled
    .select(
        "id",
        F.col("bpm").alias("m_bpm"),
        F.col("duration_sec").alias("m_duration_sec"),
        F.col("duration_sec_plus_bpm").alias("m_duration_sec_plus_bpm"),
    )
    .unpivot(
        ids=["id"],
        values=["m_bpm", "m_duration_sec", "m_duration_sec_plus_bpm"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
