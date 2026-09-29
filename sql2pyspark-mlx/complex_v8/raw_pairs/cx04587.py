from pyspark.sql import functions as F

base = (
    df_tracks
    .filter(F.lower(F.col("title")).like("%ig%"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
    )
)

without_matches = (
    base.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.array_contains(F.col("t.moods"), "workout")) & (F.upper(F.trim(F.col("t.title"))).like("S%")), "left_anti")
)

result = (
    without_matches
    .cube("is_explicit", "artist_id")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
