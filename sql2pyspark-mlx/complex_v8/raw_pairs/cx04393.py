from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_year_by_played_at = Window.partitionBy(F.year("played_at")).orderBy("played_at", "id")
w_played_at_year_by_played_at_2 = Window.partitionBy(F.year("played_at")).orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at = Window.orderBy("played_at", "id")

base_plays = (
    df_plays
    .filter(F.col("listener_id") >= 10)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.concat_ws(": ", F.col("device"), F.col("source")).alias("device_source_label"),
        F.when(F.col("ms_played") >= 218129, "A").when(F.col("ms_played") >= 73177, "B").otherwise("C").alias("ms_played_tier"),
        F.upper(F.trim(F.col("device"))).alias("device_clean"),
    )
)

windowed = (
    base_plays
    .select(
        "*",
        F.rank().over(w_played_at_year_by_played_at).alias("played_at_rank"),
        F.lead("played_at", 2).over(w_played_at_year_by_played_at).alias("next2_played_at"),
        F.last("played_at", ignorenulls=True).over(w_played_at_year_by_played_at_2).alias("last_known_played_at"),
        F.row_number().over(w_all_by_played_at).alias("played_at_seq"),
        F.first("played_at", ignorenulls=True).over(w_played_at_year_by_played_at_2).alias("first_known_played_at"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

with_stats = (
    windowed.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count_2"),
        F.count("*").alias("play_count"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
    )
)

profiled = (
    with_stats.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count_2"), F.lit(0)).alias("play_is_skipped_count_2"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.avg_play_ms_played",
    )
)

play_stats_3 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.sum("ms_played").alias("total_play_ms_played"),
        F.count("*").alias("play_count_2"),
        F.min("ms_played").alias("min_play_ms_played"),
    )
)

profiled_2 = (
    profiled.alias("s")
    .join(play_stats_3.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.total_play_ms_played",
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
        "p.min_play_ms_played",
    )
)

play_stats_4 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.max("ms_played").alias("max_play_ms_played_2"),
        F.max("played_at").alias("last_played_at"),
    )
)

summarized_rows = (
    profiled_2.alias("s")
    .join(play_stats_4.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.max_play_ms_played",
        "p.max_play_ms_played_2",
        "p.last_played_at",
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["listener_id", "play_is_skipped_count", "track_id"], ["listener_id"], ["play_is_skipped_count"], []],
        "listener_id", "play_is_skipped_count", "track_id",
    )
    .agg(
        F.grouping("listener_id").alias("is_total_listener_id"),
        F.sum(F.when(F.col("is_skipped"), 1).otherwise(0)).alias("n_is_skipped_true"),
    )
)
