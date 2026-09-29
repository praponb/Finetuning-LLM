from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_played_at_runn = Window.partitionBy("is_skipped").orderBy("played_at", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_by_played_at_full = Window.partitionBy("is_skipped").orderBy("played_at", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_played_at_ties = Window.partitionBy("is_skipped").orderBy("played_at")
w_play_id_play_source_by_ms_pl = Window.partitionBy("play_id", "play_source").orderBy("ms_played")

base_plays = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
    )
)

detail_rows = (
    base_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device") != "speaker"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
    )
)

metrics = (
    detail_rows
    .select(
        "*",
        F.first("ms_played", ignorenulls=True).over(w_is_skipped_by_played_at_runn).alias("first_known_ms_played"),
        F.nth_value("ms_played", 2).over(w_is_skipped_by_played_at_full).alias("second_ms_played"),
        F.round(F.percent_rank().over(w_is_skipped_by_played_at_ties), 3).alias("played_at_percent_rank"),
        F.round(F.percent_rank().over(w_play_id_play_source_by_ms_pl), 3).alias("ms_played_percent_rank"),
    )
)

joined_track = (
    metrics.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.play_id",
        "s.play_device",
        "s.second_ms_played",
        "s.played_at_percent_rank",
        "s.ms_played_percent_rank",
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    joined_track
    .groupBy("track_id", "played_at")
    .pivot("device", ["speaker", "desktop"])
    .agg(F.max("played_at_percent_rank"))
)
