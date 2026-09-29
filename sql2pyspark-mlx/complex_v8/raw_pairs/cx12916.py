from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.date_trunc("month", F.col("played_at")).alias("played_at_month"),
    )
)

combined_details = (
    recent_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        F.col("t.title").alias("track_title"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "track_release_date",
        F.concat(F.col("track_title"), F.lit("#"), F.col("ms_played").cast("string")).alias("track_title_ref"),
    )
    .orderBy(F.col("ms_played").asc_nulls_last())
)
