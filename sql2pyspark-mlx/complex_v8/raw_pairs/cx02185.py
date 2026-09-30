from pyspark.sql import functions as F

base = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.year(F.col("played_at")).alias("played_at_year"),
        F.when(F.col("ms_played") >= 290754, "A").when(F.col("ms_played") >= 112786, "B").otherwise("C").alias("ms_played_tier"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
)

with_details_track = (
    base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.col("t.moods").alias("track_moods"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

result = (
    with_details_track
    .select("id", "track_duration_sec", "played_at_year")
    .unpivot(
        ids=["id"],
        values=["track_duration_sec", "played_at_year"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric_name")
)
