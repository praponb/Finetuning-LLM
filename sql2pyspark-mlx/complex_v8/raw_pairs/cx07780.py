from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter((F.col("bpm") >= 89) & (F.col("duration_sec") <= 344))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
    )
)

enriched = (
    valid_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

summarized_rows = (
    enriched.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_played_at")
)

result = (
    summarized_rows
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "days_since_release_date",
        "first_played_at",
        F.when(F.col("days_since_release_date") >= 560, "large").when(F.col("days_since_release_date") >= 175, "medium").otherwise("small").alias("days_since_release_date_tier"),
    )
    .orderBy(F.col("days_since_release_date").desc())
)
