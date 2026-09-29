from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter((F.col("ms_played") <= 245877) & (F.col("source") != "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 195566).alias("ms_played_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

exploded_detail_plays = (
    prepared.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.ms_played") > 73625), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.ms_played_distance",
        "s.source_slug",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

with_activity = (
    exploded_detail_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id") <= 17), "left_anti")
)

qualified = (
    with_activity
    .crossJoin(with_activity.agg(F.min("play_ms_played").alias("overall_min_play_ms_played")))
    .filter(F.col("play_ms_played") > F.col("overall_min_play_ms_played"))
    .drop("overall_min_play_ms_played")
)

grouped = (
    qualified
    .groupBy("play_id")
    .agg(
        F.max("ms_played").alias("max_ms_played"),
        F.sum("play_ms_played").alias("total_play_ms_played"),
        F.percentile_approx("ms_played", 0.5).alias("median_ms_played"),
        F.array_sort(F.collect_set("play_device")).alias("play_device_list"),
        F.count("*").alias("row_count"),
        F.max("play_ms_played").alias("max_play_ms_played_check"),
    )
    .filter(F.col("max_play_ms_played_check") > 42041)
    .drop("max_play_ms_played_check")
)

joined_play = (
    grouped.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.play_id",
        "s.max_ms_played",
        "s.total_play_ms_played",
        "s.median_ms_played",
        "s.play_device_list",
        "s.row_count",
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

result = (
    joined_play
    .select(
        "play_id",
        "total_play_ms_played",
        "median_ms_played",
        "play_device_list",
        "row_count",
        "play_track_id",
        "play_listener_id",
        F.abs(F.col("row_count") - 5).alias("row_count_distance"),
    )
)
