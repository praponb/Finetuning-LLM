from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter((F.col("is_explicit")) & (F.col("bpm").between(76, 170)))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        F.array_contains(F.col("moods"), "workout").alias("has_workout"),
    )
)

labeled = (
    filtered_tracks
    .select(
        "*",
        F.round(F.col("bpm") / F.nullif(F.col("bpm_bucket"), F.lit(0)), 3).alias("bpm_per_bpm_bucket"),
        F.year(F.col("release_date")).alias("release_date_year"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket_2"),
    )
)

annotated = labeled.withColumn("release_date_dayofweek", F.dayofweek(F.col("release_date")))

with_children_plays = (
    annotated.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.has_workout",
        "s.bpm_bucket_2",
        "s.release_date_dayofweek",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
    )
)

result = (
    with_children_plays
    .select(
        "id",
        "play_id",
        "release_date_dayofweek",
        "bpm_bucket_2",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["release_date_dayofweek", "bpm_bucket_2"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
