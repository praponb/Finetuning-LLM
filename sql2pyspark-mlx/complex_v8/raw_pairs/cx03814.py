from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_ties = Window.orderBy("ms_played")
w_listener_id_source = Window.partitionBy("listener_id", "source")
w_all_by_avg_ms_played_ties = Window.orderBy("avg_ms_played")
w_all_by_avg_ms_played = Window.orderBy("avg_ms_played", "id")

valid_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

device_or_unknown_stats = (
    valid_plays
    .groupBy("device_or_unknown")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

outliers = (
    valid_plays.alias("s")
    .join(device_or_unknown_stats.alias("g"), F.col("g.device_or_unknown") == F.col("s.device_or_unknown"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

with_activity = (
    outliers.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (~F.array_contains(F.col("l.interests"), "live")) & (F.col("l.plan") != "free"), "left_semi")
)

windowed = (
    with_activity
    .select(
        "*",
        F.rank().over(w_all_by_ms_played_ties).alias("ms_played_rank"),
        F.size(F.collect_set("device").over(w_listener_id_source)).alias("distinct_device_in_listener_id_source"),
        F.count(F.lit(1)).over(w_listener_id_source).alias("rows_in_listener_id_source"),
        F.round(F.cume_dist().over(w_all_by_avg_ms_played_ties), 3).alias("avg_ms_played_cume_dist"),
        (F.col("ms_played") - F.lag("ms_played").over(w_all_by_avg_ms_played)).alias("ms_played_change"),
        F.size(F.collect_set("device_or_unknown").over(w_listener_id_source)).alias("distinct_device_or_unknown_in_listener_id_source"),
    )
)

derived = (
    windowed
    .filter(F.col("ms_played") >= 112786)
    .select(
        "*",
        F.dayofweek(F.col("played_at")).alias("played_at_dayofweek"),
        F.coalesce(F.col("device_or_unknown"), F.lit("UNKNOWN")).alias("device_or_unknown_or_unknown"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

profiled = (
    derived.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

with_parent = (
    profiled.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .filter(~F.col("l.country").isin("KR", "TH"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.device_or_unknown",
        "s.avg_ms_played",
        "s.ms_played_minus_avg",
        "s.ms_played_rank",
        "s.rows_in_listener_id_source",
        "s.ms_played_change",
        "s.distinct_device_or_unknown_in_listener_id_source",
        "s.played_at_dayofweek",
        "s.device_or_unknown_or_unknown",
        F.col("l.country").alias("listener_country"),
        F.col("t.title").alias("track_title"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    with_parent
    .filter(F.col("ms_played_rank") >= 5)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "avg_ms_played",
        "ms_played_minus_avg",
        "ms_played_rank",
        "distinct_device_or_unknown_in_listener_id_source",
        "device_or_unknown_or_unknown",
        "track_title",
        F.abs(F.col("track_bpm") - 100).alias("track_bpm_distance"),
    )
    .orderBy(F.col("avg_ms_played").asc_nulls_last())
)
