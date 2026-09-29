from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_id_play_id_2_by_play_pl = Window.partitionBy("play_id", "play_id_2").orderBy(F.col("play_played_at_2").desc(), "id", "play_id", "play_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_play_id_play_id_2_by_play_pl_2 = Window.partitionBy("play_id", "play_id_2").orderBy(F.col("play_played_at_2").desc(), "id", "play_id", "play_id_2").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_play_id_play_id_2_by_play_pl_3 = Window.partitionBy("play_id", "play_id_2").orderBy(F.col("play_played_at_2").desc(), "id", "play_id", "play_id_2")
w_play_id_play_id_2_by_play_pl_4 = Window.partitionBy("play_id", "play_id_2").orderBy(F.col("play_played_at_2").desc())

valid_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
    )
)

device_stats = (
    valid_plays
    .groupBy("device")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

above_average = (
    valid_plays.alias("s")
    .join(device_stats.alias("g"), F.col("g.device") == F.col("s.device"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

detail_rows_plays = (
    above_average.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.avg_ms_played",
        "s.ms_played_minus_avg",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

with_children = (
    detail_rows_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.play_id",
        "s.play_device",
        "s.play_is_skipped",
        "s.play_played_at",
        F.col("p.id").alias("play_id_2"),
        F.col("p.played_at").alias("play_played_at_2"),
        F.col("p.is_skipped").alias("play_is_skipped_2"),
        F.col("p.track_id").alias("play_track_id"),
    )
)

with_trends = (
    with_children
    .select(
        "id",
        "listener_id",
        "play_id",
        "play_device",
        "play_played_at",
        "play_id_2",
        "play_played_at_2",
        "play_is_skipped_2",
        F.first("play_played_at_2", ignorenulls=True).over(w_play_id_play_id_2_by_play_pl).alias("first_known_play_played_at_2"),
        F.nth_value("play_played_at_2", 2).over(w_play_id_play_id_2_by_play_pl_2).alias("second_play_played_at_2"),
        F.row_number().over(w_play_id_play_id_2_by_play_pl_3).alias("seq_no"),
        F.last("play_played_at", ignorenulls=True).over(w_play_id_play_id_2_by_play_pl).alias("last_known_play_played_at"),
        F.round(F.percent_rank().over(w_play_id_play_id_2_by_play_pl_4), 3).alias("play_played_at_2_percent_rank"),
    )
)

result = (
    with_trends
    .groupBy("play_played_at_2", "play_played_at")
    .pivot("play_device", ["phone", "car"])
    .agg(F.avg("seq_no"))
    .orderBy("play_played_at_2", "play_played_at")
)
