from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_play_devices = Window.partitionBy("distinct_play_devices")
w_all_by_max_play_ms_played_fu = Window.orderBy("max_play_ms_played", "play_played_at_year").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_max_play_ms_played = Window.orderBy("max_play_ms_played", "play_played_at_year")

valid = (
    df_plays
    .filter((F.col("listener_id") >= 14) & (F.col("device") != "desktop"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

with_children_plays = (
    valid.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device_slug",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
    )
)

per_group = (
    with_children_plays
    .groupBy(F.year("play_played_at").alias("play_played_at_year"))
    .agg(
        F.max("play_ms_played").alias("max_play_ms_played"),
        F.count("*").alias("cnt"),
        F.round(F.avg("play_ms_played"), 2).alias("avg_play_ms_played"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.countDistinct("play_device").alias("distinct_play_devices"),
    )
)

windowed = (
    per_group
    .select(
        "*",
        F.min("distinct_track_ids").over(w_distinct_play_devices).alias("distinct_play_devices_min_distinct_track_ids"),
        F.nth_value("cnt", 2).over(w_all_by_max_play_ms_played_fu).alias("second_cnt"),
        F.ntile(10).over(w_all_by_max_play_ms_played).alias("max_play_ms_played_decile"),
        F.sum("play_played_at_year").over(w_distinct_play_devices).alias("distinct_play_devices_sum_play_played_at_year"),
        F.round(F.avg("max_play_ms_played").over(w_distinct_play_devices), 2).alias("distinct_play_devices_avg_max_play_ms_played"),
        F.round(F.col("cnt") - F.avg("cnt").over(w_distinct_play_devices), 2).alias("cnt_vs_distinct_play_devices_avg"),
    )
)

result = (
    windowed
    .select(
        "play_played_at_year",
        "second_cnt",
        "distinct_play_devices",
        "distinct_play_devices_sum_play_played_at_year",
        "max_play_ms_played_decile",
    )
    .unpivot(
        ids=["play_played_at_year"],
        values=["second_cnt", "distinct_play_devices", "distinct_play_devices_sum_play_played_at_year", "max_play_ms_played_decile"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("play_played_at_year", "field")
)
