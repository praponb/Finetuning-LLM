from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_full = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_running = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_dayofweek_played_a = Window.partitionBy("played_at_dayofweek", F.year("played_at")).orderBy(F.col("played_at").desc(), "id")

scoped = (
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
        F.dayofweek(F.col("played_at")).alias("played_at_dayofweek"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
    )
)

scored = (
    scoped
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "played_at_dayofweek",
        "source_len",
        "source_prefix",
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.last("played_at_year").over(w_all_by_played_at_full).alias("last_played_at_year"),
        F.min("source_len").over(w_all_by_played_at_running).alias("min_source_len_so_far"),
        F.row_number().over(w_played_at_dayofweek_played_a).alias("seq_no"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.min("ms_played").alias("min_play_ms_played"),
    )
)

summarized_rows = (
    with_windows.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.min_play_ms_played",
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["seq_no", "play_count", "played_at_dayofweek"], ["seq_no"], []],
        "seq_no", "play_count", "played_at_dayofweek",
    )
    .agg(F.max("min_source_len_so_far").alias("max_min_source_len_so_far"))
    .orderBy(
        F.col("seq_no").asc_nulls_last(),
        F.col("play_count").asc_nulls_last(),
        F.col("played_at_dayofweek").asc_nulls_last(),
    )
)
