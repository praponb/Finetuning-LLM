from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "chill")) & (F.upper(F.trim(F.col("title"))).like("C%")))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("duration_sec") >= 352, "HIGH").when(F.col("duration_sec") >= 184, "MEDIUM").otherwise("LOW").alias("duration_sec_tier"),
        F.when(F.col("bpm") >= 147, "large").when(F.col("bpm") >= 96, "medium").otherwise("small").alias("bpm_tier"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.sum("bpm").alias("total_track_bpm"))
)

summarized_rows = (
    valid.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.total_track_bpm")
)

result = (
    summarized_rows
    .groupingSets(
        [["duration_sec_tier", "artist_id", "bpm_tier"], ["duration_sec_tier"], []],
        "duration_sec_tier", "artist_id", "bpm_tier",
    )
    .agg(
        F.sum(F.when(~F.col("is_explicit"), F.col("bpm")).otherwise(0)).alias("bpm_not_is_explicit"),
        F.round(100 * F.count(F.when(~F.col("is_explicit"), 1)) / F.count("*"), 1).alias("pct_not_is_explicit"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
    )
    .orderBy(
        F.col("duration_sec_tier").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
        F.col("bpm_tier").asc_nulls_last(),
    )
)
