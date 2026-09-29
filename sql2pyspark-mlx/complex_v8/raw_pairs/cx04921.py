from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_track_id_by_play = Window.partitionBy("listener_id", "track_id").orderBy("played_at", "id")
w_track_id_listener_id_by_max = Window.partitionBy("track_id", "listener_id").orderBy(F.col("max_ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_track_id_by_play_2 = Window.partitionBy("listener_id", "track_id").orderBy("played_at")
w_source = Window.partitionBy("source")
w_all_by_played_at = Window.orderBy(F.col("played_at").desc_nulls_first(), "id")

cleaned_plays = (
    df_plays
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

filtered = (
    cleaned_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.ms_played") >= 107912), "left_anti")
)

listener_id_stats = (
    filtered
    .groupBy("listener_id")
    .agg(F.max("ms_played").alias("max_ms_played"))
)

vs_group = (
    filtered.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.ms_played") == F.col("g.max_ms_played"))
    .select(
        "s.*",
        "g.max_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.max_ms_played"), 2).alias("ms_played_minus_max"),
    )
)

windowed = (
    vs_group
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "max_ms_played",
        "ms_played_minus_max",
        F.ntile(3).over(w_listener_id_track_id_by_play).alias("played_at_tercile"),
        F.last("played_at").over(w_track_id_listener_id_by_max).alias("last_played_at"),
        F.rank().over(w_listener_id_track_id_by_play_2).alias("played_at_rank"),
        F.count(F.lit(1)).over(w_source).alias("rows_in_source"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_source), 2).alias("ms_played_vs_source_avg"),
        (F.col("ms_played_minus_max") - F.lag("ms_played_minus_max").over(w_all_by_played_at)).alias("ms_played_minus_max_change"),
    )
)

result = (
    windowed
    .rollup("played_at_rank", "is_skipped")
    .agg(
        F.grouping("played_at_rank").alias("is_total_played_at_rank"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
    )
)
