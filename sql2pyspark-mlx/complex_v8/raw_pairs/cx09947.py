from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device")
w_all_by_ms_played_filled_full = Window.orderBy("ms_played_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played_filled = Window.orderBy("ms_played_filled", "id")

scoped_plays = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-18 11:00:00").cast("timestamp")) & (F.col("listener_id") <= 10))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("ms_played").cast("string")).alias("source_ref"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

windowed = (
    scoped_plays
    .select(
        "*",
        F.size(F.collect_set("track_id").over(w_device)).alias("distinct_track_id_in_device"),
        F.nth_value("ms_played", 2).over(w_all_by_ms_played_filled_full).alias("second_ms_played"),
        F.round(F.cume_dist().over(w_all_by_ms_played_filled), 3).alias("ms_played_filled_cume_dist"),
        F.ntile(4).over(w_all_by_ms_played_filled).alias("ms_played_filled_quartile"),
    )
)

qualified = (
    windowed.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.upper(F.trim(F.col("l.username"))).like("H%")) & (F.year(F.col("l.signup_date")) == 2024), "left_semi")
)

line_level_plays = (
    qualified.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.source_ref",
        "s.ms_played_filled",
        "s.ms_played_filled_cume_dist",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    line_level_plays
    .groupBy("source", "play_played_at")
    .pivot("device", ["tv", "speaker", "car"])
    .agg(F.min("ms_played"))
)
