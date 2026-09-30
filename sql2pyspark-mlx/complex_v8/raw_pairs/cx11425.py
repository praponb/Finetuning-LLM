from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_ms_played = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc(), "id")
w_is_skipped_track_id_by_first = Window.partitionBy("is_skipped", "track_id").orderBy("first_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played_distance_mo = Window.orderBy("ms_played_distance", "id").rowsBetween(-1, Window.currentRow)
w_first_played_at_to_date_is_s = Window.partitionBy(F.to_date("first_played_at"), "is_skipped")
w_latest = Window.partitionBy("device").orderBy("first_played_at", "id")

filtered = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
        F.abs(F.col("ms_played") - 130888).alias("ms_played_distance"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

with_counts = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.first_played_at")
)

with_windows = (
    with_counts
    .select(
        "id",
        "listener_id",
        "ms_played",
        "device",
        "source",
        "source_prefix",
        "ms_played_distance",
        "first_played_at",
        F.lag("played_at", 2).over(w_track_id_by_ms_played).alias("prev2_played_at"),
        F.nth_value("ms_played_distance", 2).over(w_is_skipped_track_id_by_first).alias("second_ms_played_distance"),
        F.round(F.avg("ms_played").over(w_all_by_ms_played_distance_mo), 2).alias("ms_played_avg_2"),
        F.count(F.lit(1)).over(w_first_played_at_to_date_is_s).alias("rows_in_first_played_at_to_date_is_skipped"),
    )
)

latest_per_key = (
    with_windows
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

play_stats_2 = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(~F.col("source").isin("album", "radio"), 1)).alias("play_source_count"),
        F.max("ms_played").alias("max_play_ms_played"),
        F.count("*").alias("play_count"),
    )
)

with_stats = (
    latest_per_key.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        "p.max_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

qualified = (
    with_stats.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.track_id").isNotNull()) & (F.col("p.played_at") >= F.lit("2025-01-19 14:00:00").cast("timestamp")), "left_anti")
)

result = (
    qualified
    .filter(F.col("source_prefix").isNotNull())
    .select(
        "id",
        "listener_id",
        "ms_played",
        "device",
        "source",
        "source_prefix",
        "ms_played_distance",
        "first_played_at",
        "prev2_played_at",
        "second_ms_played_distance",
        "ms_played_avg_2",
        "rows_in_first_played_at_to_date_is_skipped",
        "play_source_count",
        "play_count",
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
    .orderBy(F.col("device").desc())
)
