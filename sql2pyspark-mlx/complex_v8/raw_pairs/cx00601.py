from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_play_count_2_full = Window.orderBy("play_count_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_avg_play_ms_played = Window.orderBy("avg_play_ms_played", "id")
w_all_by_avg_play_ms_played_ti = Window.orderBy("avg_play_ms_played")
w_track_id = Window.partitionBy("track_id")
w_all_by_play_count_2 = Window.orderBy("play_count_2", "id")

prepared = (
    df_plays
    .filter((F.col("track_id").isNotNull()) | (F.col("listener_id") <= 2))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

with_counts = (
    prepared.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.avg_play_ms_played", "p.total_play_ms_played")
)

play_stats_2 = (
    df_plays
    .groupBy("listener_id")
    .agg(F.count("*").alias("play_count"), F.count("*").alias("play_count_2"))
)

profiled = (
    with_counts.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

with_trends = (
    profiled
    .select(
        "*",
        F.nth_value("play_count_2", 2).over(w_all_by_play_count_2_full).alias("second_play_count_2"),
        F.round(F.cume_dist().over(w_all_by_avg_play_ms_played), 3).alias("avg_play_ms_played_cume_dist"),
        F.rank().over(w_all_by_avg_play_ms_played_ti).alias("avg_play_ms_played_rank"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.row_number().over(w_all_by_play_count_2).alias("seq_no"),
    )
)

result = (
    with_trends
    .cube("is_skipped_flag", "seq_no")
    .agg(F.array_sort(F.collect_set("device")).alias("device_list"))
)
