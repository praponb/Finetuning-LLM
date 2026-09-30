from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_device = Window.partitionBy("play_device")
w_all_by_play_ms_played = Window.orderBy("play_ms_played", "id", "play_id")

scoped = (
    df_plays
    .filter((~F.col("source").isin("radio", "playlist")) & (F.col("played_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

line_level = (
    scoped.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.days_since_played_at",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

ranked = (
    line_level
    .withColumn("days_since_played_at_share_of_play_device", F.round(F.col("days_since_played_at") / F.nullif(F.sum("days_since_played_at").over(w_play_device), F.lit(0)), 4))
    .withColumn("next_played_at", F.lead("played_at").over(w_all_by_play_ms_played))
)

line_level_plays = (
    ranked.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.days_since_played_at",
        "s.play_id",
        "s.play_track_id",
        "s.play_ms_played",
        "s.play_is_skipped",
        "s.next_played_at",
        F.col("p.id").alias("play_id_2"),
        F.col("p.device").alias("play_device_2"),
        F.col("p.ms_played").alias("play_ms_played_2"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.track_id").alias("play_track_id_2"),
    )
)

result = (
    line_level_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "days_since_played_at",
        "play_id",
        "play_is_skipped",
        "next_played_at",
        "play_id_2",
        "play_played_at",
        "play_track_id_2",
        F.year(F.col("next_played_at")).alias("next_played_at_year"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("play_played_at")).alias("days_since_play_played_at"),
    )
)
