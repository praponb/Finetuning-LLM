from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_device = Window.partitionBy("listener_id", "device")

prepared_plays = (
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
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.max("played_at").alias("last_played_at_2"),
        F.count("*").alias("play_count"),
    )
)

with_counts = (
    prepared_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        "p.last_played_at_2",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

ranked = (
    with_counts
    .select(
        "id",
        "played_at",
        "is_skipped",
        "source",
        "last_played_at_2",
        "play_count",
        F.round(F.col("play_count") - F.avg("play_count").over(w_listener_id_device), 2).alias("play_count_vs_listener_id_device_avg"),
        F.size(F.collect_set("track_id").over(w_listener_id_device)).alias("distinct_track_id_in_listener_id_device"),
    )
)

filtered = (
    ranked
    .crossJoin(ranked.agg(F.min("distinct_track_id_in_listener_id_device").alias("overall_min_distinct_track_id_in_listener_id_dev")))
    .filter(F.col("distinct_track_id_in_listener_id_device") > F.col("overall_min_distinct_track_id_in_listener_id_dev"))
    .drop("overall_min_distinct_track_id_in_listener_id_dev")
)

result = (
    filtered
    .select("id", "distinct_track_id_in_listener_id_device", "play_count")
    .unpivot(
        ids=["id"],
        values=["distinct_track_id_in_listener_id_device", "play_count"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric_name")
)
