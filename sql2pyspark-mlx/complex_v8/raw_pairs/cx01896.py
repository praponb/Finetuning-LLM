from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter((~F.col("is_explicit")) | (~F.array_contains(F.col("moods"), "focus")))
)

screened = (
    scoped_tracks.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") < 156), "left_anti")
)

scored = (
    screened
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.when(F.col("duration_sec") >= 312, "HIGH").when(F.col("duration_sec") >= 154, "MEDIUM").otherwise("LOW").alias("duration_sec_tier"),
    )
)

enriched = (
    scored.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.duration_sec_bucket",
        "s.duration_sec_tier",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

result = (
    enriched
    .groupingSets(
        [["artist_id", "is_explicit", "duration_sec_tier"], ["artist_id"]],
        "artist_id", "is_explicit", "duration_sec_tier",
    )
    .agg(F.countDistinct("artist_id").alias("distinct_artist_ids"))
)
