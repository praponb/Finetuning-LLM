from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_track_id_by_ms_p = Window.partitionBy("listener_id", "track_id").orderBy("ms_played")
w_source_played_at_month = Window.partitionBy("source", F.month("played_at"))

cleaned = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws("-", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

ranked = (
    cleaned
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "device_source_label",
        F.round(F.cume_dist().over(w_listener_id_track_id_by_ms_p), 3).alias("ms_played_cume_dist"),
        F.size(F.collect_set("device").over(w_source_played_at_month)).alias("distinct_device_in_source_played_at_month"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count"),
    )
)

with_stats = (
    ranked.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        "p.last_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

stats = (
    with_stats
    .groupBy("device", "distinct_device_in_source_played_at_month")
    .agg(
        F.min("first_played_at").alias("first_first_played_at"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("device").isin("tv", "speaker"), F.col("ms_played"))).alias("ms_played_device_in_tv_speaker"),
        F.count(F.when(F.col("device").isin("speaker", "car"), 1)).alias("n_device_in_speaker_car"),
    )
)

result = (
    stats
    .select(
        "device",
        "distinct_device_in_source_played_at_month",
        "distinct_track_ids",
        "n_rows",
        "n_device_in_speaker_car",
    )
    .unpivot(
        ids=["device", "distinct_device_in_source_played_at_month"],
        values=["distinct_track_ids", "n_rows", "n_device_in_speaker_car"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .orderBy("device", "distinct_device_in_source_played_at_month", "metric_name")
)
