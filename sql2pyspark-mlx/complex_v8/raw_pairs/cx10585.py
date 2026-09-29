from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("S%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
    )
)

without_matches = (
    prepared_tracks.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id").isNotNull()) & (F.col("p.device") == "car"), "left_anti")
)

result = (
    without_matches
    .select("id", "duration_sec", "bpm")
    .unpivot(
        ids=["id"],
        values=["duration_sec", "bpm"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric")
)
