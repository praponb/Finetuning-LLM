from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_listener_id_by_ms_pla = Window.partitionBy("device", "listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_len_source = Window.partitionBy("source_len", "source")
w_played_at_month_by_play_coun = Window.partitionBy(F.month("played_at")).orderBy(F.col("play_count_2").desc(), "id")
w_played_at_month_by_play_coun_2 = Window.partitionBy(F.month("played_at")).orderBy(F.col("play_count_2").desc())
w_device = Window.partitionBy("device")

cleaned = (
    df_plays
    .filter((F.month(F.col("played_at")).isin(1, 2)) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.nth_value("played_at", 2).over(w_device_listener_id_by_ms_pla).alias("second_played_at"),
        F.count(F.lit(1)).over(w_source_len_source).alias("rows_in_source_len_source"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count_2"),
    )
)

summarized_rows = (
    sequenced.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.last_played_at",
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

with_trends = (
    summarized_rows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        "second_played_at",
        "rows_in_source_len_source",
        "play_count",
        F.ntile(4).over(w_played_at_month_by_play_coun).alias("play_count_2_quartile"),
        F.round(F.percent_rank().over(w_played_at_month_by_play_coun_2), 3).alias("play_count_2_percent_rank"),
        F.sum("source_len").over(w_device).alias("device_sum_source_len"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["play_count_2_quartile", "listener_id"], ["play_count_2_quartile"]],
        "play_count_2_quartile", "listener_id",
    )
    .agg(
        F.percentile_approx("play_count_2_percent_rank", 0.5).alias("median_play_count_2_percent_rank"),
    )
    .orderBy(
        F.col("play_count_2_quartile").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
    )
)
