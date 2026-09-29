from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_source_or_unknown_b = Window.partitionBy("track_id", "source_or_unknown").orderBy(F.col("played_at").desc(), "id")
w_track_id_source_or_unknown_b_2 = Window.partitionBy("track_id", "source_or_unknown").orderBy(F.col("played_at").desc())

filtered_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

with_trends = (
    filtered_plays
    .select(
        "*",
        F.ntile(10).over(w_track_id_source_or_unknown_b).alias("played_at_decile"),
        F.round(F.percent_rank().over(w_track_id_source_or_unknown_b_2), 3).alias("played_at_percent_rank"),
        F.lag("ms_played", 3).over(w_track_id_source_or_unknown_b).alias("prev3_ms_played"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.max("ms_played").alias("max_play_ms_played"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("play_device_count"),
    )
)

summarized_rows = (
    with_trends.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.max_play_ms_played",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.sum("ms_played").alias("total_play_ms_played"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

profiled = (
    summarized_rows.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.total_play_ms_played",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

result = (
    profiled
    .rollup("play_is_skipped_count", "track_id")
    .agg(
        F.round(100 * F.count(F.when(F.col("source") == "search", 1)) / F.count("*"), 1).alias("pct_source_search"),
        F.sum(F.when(~F.col("source").isin("radio", "search"), F.col("played_at_percent_rank")).otherwise(0)).alias("played_at_percent_rank_source_not_in_radio_searc"),
    )
)
