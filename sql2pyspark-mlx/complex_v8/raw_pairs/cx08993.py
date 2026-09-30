from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_ms_played = Window.partitionBy("track_id").orderBy("ms_played", "id")
w_dedup = Window.partitionBy("listener_id").orderBy("listener_birth_year", "id")

recent_plays = (
    df_plays
    .filter((~F.col("device").isin("phone", "desktop")) | (F.col("ms_played") >= 107912))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
    )
)

with_activity = (
    recent_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device").isin("tv", "car", "desktop")) & (F.col("p.ms_played") < 223262), "left_semi")
    .filter(F.col("played_at") < F.lit("2025-02-24 11:00:00").cast("timestamp"))
)

combined_details = (
    with_activity.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(~F.array_contains(F.col("l.interests"), "kpop"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.played_at_label",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

sequenced = (
    combined_details
    .select(
        "*",
        F.round(F.cume_dist().over(w_track_id_by_ms_played), 3).alias("ms_played_cume_dist"),
        F.lag("ms_played", 3, 0).over(w_track_id_by_ms_played).alias("prev3_ms_played"),
    )
)

latest = (
    sequenced
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .filter(F.col("ms_played_cume_dist") > 0.1)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "played_at_label",
        "listener_birth_year",
        "ms_played_cume_dist",
        F.abs(F.col("ms_played") - 129336).alias("ms_played_distance"),
    )
)
