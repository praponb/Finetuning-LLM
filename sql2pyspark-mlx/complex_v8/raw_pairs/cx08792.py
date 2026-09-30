from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played_filled").desc(), F.col("id").desc())

scoped = (
    df_plays
    .filter((F.to_date(F.col("played_at")) < F.lit("2025-02-18").cast("date")) & (F.col("ms_played") > 120663))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

latest = (
    scoped
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched_track = (
    latest.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .filter(F.col("t.is_explicit"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.ms_played_filled",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("l.username").alias("listener_username"),
    )
)

result = (
    enriched_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        "ms_played_filled",
        "track_is_explicit",
        "listener_username",
        (F.floor(F.col("ms_played_filled") / 50000) * 50000).alias("ms_played_filled_bucket"),
    )
    .orderBy(F.col("listener_id").desc(), F.col("source").asc())
)
