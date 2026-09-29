from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.col("artist_id") >= 7))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label_2"),
    )
)

with_details = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.release_date_label_2",
        "a.artist_name",
    )
)

result = (
    with_details
    .select("id", "duration_sec", "bpm")
    .unpivot(
        ids=["id"],
        values=["duration_sec", "bpm"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric")
)
