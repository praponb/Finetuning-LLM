from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_is_skipped_played_at_month_b = Window.partitionBy("is_skipped", F.month("played_at")).orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_ties = Window.orderBy("played_at")
w_listener_id_source = Window.partitionBy("listener_id", "source")
w_listener_id_device_or_unknow = Window.partitionBy("listener_id", "device_or_unknown").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_plays = (
    df_plays
    .filter((F.col("source") != "search") | (F.col("device") == "tv"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 225733).alias("ms_played_distance"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.date_trunc("week", F.col("played_at")).alias("played_at_week"),
    )
)

ranked = (
    recent_plays
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "ms_played_distance",
        "device_or_unknown",
        "played_at_week",
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.max("ms_played_distance").over(w_is_skipped_played_at_month_b).alias("max_ms_played_distance_so_far"),
    )
)

windowed = (
    ranked
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_played_at_ties), 3).alias("played_at_cume_dist"),
        F.size(F.collect_set("device_or_unknown").over(w_listener_id_source)).alias("distinct_device_or_unknown_in_listener_id_source"),
        F.count(F.lit(1)).over(w_listener_id_source).alias("rows_in_listener_id_source"),
        F.round(F.col("ms_played_distance") / F.nullif(F.sum("ms_played_distance").over(w_listener_id_source), F.lit(0)), 4).alias("ms_played_distance_share_of_listener_id_source"),
        F.nth_value("ms_played", 2).over(w_listener_id_device_or_unknow).alias("second_ms_played"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

profiled = (
    windowed.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.total_play_ms_played",
    )
)

result = (
    profiled
    .groupingSets(
        [["distinct_device_or_unknown_in_listener_id_source", "play_count", "is_skipped"], ["distinct_device_or_unknown_in_listener_id_source"]],
        "distinct_device_or_unknown_in_listener_id_source", "play_count", "is_skipped",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("source") == "radio", 1)) / F.count("*"), 1).alias("pct_source_radio"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
    )
    .orderBy(
        F.col("distinct_device_or_unknown_in_listener_id_source").asc_nulls_last(),
        F.col("play_count").asc_nulls_last(),
        F.col("is_skipped").asc_nulls_last(),
    )
)
