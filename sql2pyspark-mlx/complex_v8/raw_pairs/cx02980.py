from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_is_skipped_by_ms_play = Window.partitionBy("source", "is_skipped").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_is_skipped_by_ms_play_2 = Window.partitionBy("source", "is_skipped").orderBy("ms_played", "id")
w_is_skipped_device_by_ms_play = Window.partitionBy("is_skipped", "device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_device_by_ms_play_2 = Window.partitionBy("is_skipped", "device").orderBy("ms_played", "id")

recent = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (~F.col("source").isin("playlist", "album")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
        F.abs(F.col("ms_played") - 223828).alias("ms_played_distance"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
)

with_details_listener = (
    recent.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.device_prefix",
        "s.ms_played_distance",
        "s.played_at_label",
        F.col("l.interests").alias("listener_interests"),
    )
)

ranked = (
    with_details_listener
    .withColumn("first_known_ms_played_distance", F.first("ms_played_distance", ignorenulls=True).over(w_source_is_skipped_by_ms_play))
    .withColumn("prev_ms_played", F.lag("ms_played").over(w_source_is_skipped_by_ms_play_2))
)

windowed = (
    ranked
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device_prefix",
        "played_at_label",
        "first_known_ms_played_distance",
        "prev_ms_played",
        F.nth_value("ms_played_distance", 2).over(w_is_skipped_device_by_ms_play).alias("second_ms_played_distance"),
        F.lag("ms_played", 1, 0).over(w_is_skipped_device_by_ms_play_2).alias("prev_ms_played_2"),
    )
)

listener_id_stats = (
    windowed
    .groupBy("listener_id")
    .agg(F.max("prev_ms_played_2").alias("max_prev_ms_played_2"))
)

vs_group = (
    windowed.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.prev_ms_played_2") == F.col("g.max_prev_ms_played_2"))
    .select(
        "s.*",
        "g.max_prev_ms_played_2",
        F.round(F.col("s.prev_ms_played_2") - F.col("g.max_prev_ms_played_2"), 2).alias("prev_ms_played_2_minus_max"),
    )
)

track_id_stats = (
    vs_group
    .groupBy("track_id")
    .agg(F.percentile_approx("max_prev_ms_played_2", 0.5).alias("median_max_prev_ms_played_2"))
)

above_average = (
    vs_group.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.max_prev_ms_played_2") >= F.col("g.median_max_prev_ms_played_2"))
    .select(
        "s.*",
        "g.median_max_prev_ms_played_2",
        F.round(F.col("s.max_prev_ms_played_2") - F.col("g.median_max_prev_ms_played_2"), 2).alias("max_prev_ms_played_2_minus_median"),
    )
)

listener_id_stats_2 = (
    above_average
    .groupBy("listener_id")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

compared = (
    above_average.alias("s")
    .join(listener_id_stats_2.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at_label",
        "prev_ms_played",
        "second_ms_played_distance",
        "prev_ms_played_2",
        "max_prev_ms_played_2",
        "prev_ms_played_2_minus_max",
        "max_prev_ms_played_2_minus_median",
        "ms_played_minus_avg",
        F.substring(F.trim(F.col("device_prefix")), 1, 4).alias("device_prefix_prefix"),
    )
    .orderBy(F.col("prev_ms_played_2_minus_max").desc(), F.col("id").desc())
)
