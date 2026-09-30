from pyspark.sql import functions as F

cleaned = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.col("duration_sec") >= 154))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
    )
)

with_children_plays = (
    cleaned.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.release_date_quarter",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
    )
)

filtered = (
    with_children_plays.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("t.title"))).like("E%")) & (F.year(F.col("t.release_date")) == 2024), "left_anti")
)

result = (
    filtered
    .select(
        "id",
        "play_id",
        "release_date_quarter",
        "bpm",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["release_date_quarter", "bpm"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "play_id", "metric_name")
)
