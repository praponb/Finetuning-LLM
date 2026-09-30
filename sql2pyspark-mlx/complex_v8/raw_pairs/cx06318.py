from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-02-18 12:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.month(F.col("played_at")).alias("played_at_month"),
    )
)

screened = (
    scoped_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.source").isNotNull()) & (F.col("p.is_skipped")), "left_anti")
)

with_details_track = (
    screened.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select("s.*", F.col("t.release_date").alias("track_release_date"))
)

result = (
    with_details_track
    .groupBy("track_release_date", "track_id")
    .pivot("device", ["car", "desktop", "phone", "speaker"])
    .agg(F.sum("ms_played").alias("total"), F.max("ms_played").alias("max_v"))
    .orderBy("track_release_date", "track_id")
)
