from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_count_by_avg_ms_played = Window.partitionBy("play_count").orderBy(F.col("avg_ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_play_count_by_avg_ms_played_2 = Window.partitionBy("play_count").orderBy(F.col("avg_ms_played").desc(), "id").rowsBetween(-2, -1)
w_device = Window.partitionBy("device")
w_second_play_count_listener_i = Window.partitionBy("second_play_count", "listener_id").orderBy(F.col("avg_ms_played").asc_nulls_last(), "id")

base_plays = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

track_id_stats = (
    base_plays
    .groupBy("track_id")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

compared = (
    base_plays.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

labeled = (
    compared
    .filter(F.col("ms_played") > 71476)
    .select(
        "*",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.round(F.col("avg_ms_played") / F.nullif(F.col("ms_played"), F.lit(0)), 3).alias("avg_ms_played_per_ms_played"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.min("played_at").alias("first_played_at"),
    )
)

profiled = (
    labeled.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.first_played_at",
    )
)

windowed = (
    profiled
    .select(
        "*",
        F.nth_value("play_count", 2).over(w_play_count_by_avg_ms_played).alias("second_play_count"),
        F.round(F.avg("ms_played_minus_avg").over(w_play_count_by_avg_ms_played_2), 2).alias("ms_played_minus_avg_avg_2_prior"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.sum("second_play_count").over(w_device).alias("device_sum_second_play_count"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.row_number().over(w_second_play_count_listener_i).alias("rn"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "avg_ms_played",
        "ms_played_minus_avg",
        "device_or_unknown",
        "avg_ms_played_per_ms_played",
        "first_played_at",
        "second_play_count",
        "ms_played_minus_avg_avg_2_prior",
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
    )
    .orderBy(F.col("listener_id").desc())
)
