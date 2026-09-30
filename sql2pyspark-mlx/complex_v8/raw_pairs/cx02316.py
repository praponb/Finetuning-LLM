from pyspark.sql import functions as F

valid = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("ms_played") >= 145720))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.upper(F.trim(F.col("device"))).alias("device_clean"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
)

with_parent = (
    valid.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.artist_id").isNotNull())
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.days_since_played_at",
        "s.played_at_label",
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

labeled = (
    with_parent
    .filter(F.col("days_since_played_at") < 595)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "days_since_played_at",
        "track_artist_id",
        (F.floor(F.col("days_since_played_at") / 100) * 100).alias("days_since_played_at_bucket"),
    )
)

result = (
    labeled
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "days_since_played_at",
        "days_since_played_at_bucket",
        F.date_trunc("week", F.col("played_at")).alias("played_at_week"),
        (F.floor(F.col("days_since_played_at") / 100) * 100).alias("days_since_played_at_bucket_2"),
    )
    .orderBy(F.col("played_at").desc_nulls_first())
)
