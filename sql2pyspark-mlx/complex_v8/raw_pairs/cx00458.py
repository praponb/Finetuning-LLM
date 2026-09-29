from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_to_source_l = Window.orderBy("ms_played_to_source_len_ratio", "played_at_month", "listener_id", "is_skipped").rowsBetween(-2, Window.currentRow)
w_all_by_ms_played_to_source_l_2 = Window.orderBy("ms_played_to_source_len_ratio")
w_all_by_ms_played_to_source_l_3 = Window.orderBy("ms_played_to_source_len_ratio", "played_at_month", "listener_id", "is_skipped").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_median_source_len = Window.partitionBy("is_skipped", "median_source_len").orderBy(F.col("ms_played_to_source_len_ratio").asc_nulls_last(), "played_at_month", "listener_id", "is_skipped").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_median_source_le = Window.partitionBy("listener_id", "median_source_len").orderBy("ms_played_to_source_len_ratio", "played_at_month", "listener_id", "is_skipped").rowsBetween(-3, Window.currentRow)

prepared_plays = (
    df_plays
    .filter((F.col("ms_played").between(112786, 261579)) & (F.col("played_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

rollup_base = (
    prepared_plays
    .groupBy(
        F.date_trunc("month", "played_at").alias("played_at_month"),
        "listener_id",
        "is_skipped",
    )
    .agg(
        F.round(F.sum("ms_played") / F.nullif(F.sum("source_len"), F.lit(0)), 3).alias("ms_played_to_source_len_ratio"),
        F.percentile_approx("source_len", 0.5).alias("median_source_len"),
    )
)

qualified = (
    rollup_base.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.size(F.col("l.interests")) >= 1) & (F.col("l.plan").isin("family", "student", "premium")), "left_anti")
)

with_trends = (
    qualified
    .select(
        "*",
        F.max("median_source_len").over(w_all_by_ms_played_to_source_l).alias("median_source_len_max_3"),
        F.dense_rank().over(w_all_by_ms_played_to_source_l_2).alias("ms_played_to_source_len_ratio_dense_rank"),
        F.last("median_source_len").over(w_all_by_ms_played_to_source_l_3).alias("last_median_source_len"),
        F.first("median_source_len").over(w_is_skipped_median_source_len).alias("first_median_source_len"),
        F.round(F.avg("median_source_len").over(w_listener_id_median_source_le), 2).alias("median_source_len_avg_4"),
    )
)

without_matches = (
    with_trends.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (~F.col("p.is_skipped")), "left_semi")
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.max("ms_played").alias("max_play_ms_played"),
        F.max("played_at").alias("last_played_at"),
    )
)

profiled = (
    without_matches.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.max_play_ms_played",
        "p.last_played_at",
    )
)

result = (
    profiled
    .filter(~F.col("is_skipped"))
    .select(
        "played_at_month",
        "listener_id",
        "is_skipped",
        "median_source_len_max_3",
        "ms_played_to_source_len_ratio_dense_rank",
        "last_median_source_len",
        "first_median_source_len",
        "play_count",
        "max_play_ms_played",
        "last_played_at",
        (F.floor(F.col("median_source_len_max_3") / 5) * 5).alias("median_source_len_max_3_bucket"),
    )
    .orderBy(
        F.col("median_source_len_max_3").asc_nulls_last(),
        "last_median_source_len",
    )
)
