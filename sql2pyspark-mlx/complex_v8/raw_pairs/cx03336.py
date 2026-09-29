from pyspark.sql import functions as F

filtered = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 2) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.round(F.avg("bpm"), 2).alias("avg_track_bpm"))
)

profiled = (
    filtered.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.avg_track_bpm")
)

derived = (
    profiled
    .select(
        "id",
        "release_date",
        "bpm",
        "moods",
        "days_since_release_date",
        "avg_track_bpm",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.month(F.col("release_date")).alias("release_date_month"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
)

summary = (
    derived
    .groupBy(
        F.date_trunc("month", "release_date").alias("release_date_month_2"),
        "release_date_month",
    )
    .agg(
        F.min("release_date").alias("first_release_date"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
        F.sum("days_since_release_date").alias("total_days_since_release_date"),
        F.min("bpm_filled").alias("min_bpm_filled"),
        F.round(F.avg("bpm_filled"), 2).alias("avg_bpm_filled"),
    )
)

result = (
    summary
    .select(
        "release_date_month_2",
        "release_date_month",
        "first_release_date",
        "total_days_since_release_date",
        "min_bpm_filled",
        "avg_bpm_filled",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("first_release_date")).alias("days_since_first_release_date"),
    )
    .orderBy(
        F.col("avg_bpm_filled").desc(),
        "min_bpm_filled",
        "release_date_month_2",
        "release_date_month",
    )
    .limit(20)
)
