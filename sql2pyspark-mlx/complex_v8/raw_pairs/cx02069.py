from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_played_at_plus_1d = Window.partitionBy("is_skipped", F.month("played_at_plus_1d"))
w_device_by_ms_played_running = Window.partitionBy("device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id = Window.partitionBy("listener_id")
w_all_by_rows_in_is_skipped_pl = Window.orderBy(F.col("rows_in_is_skipped_played_at_plus_1d_month").asc_nulls_last())
w_rows_in_is_skipped_played_at = Window.partitionBy("rows_in_is_skipped_played_at_plus_1d_month_rank", "source").orderBy(F.col("ms_played_vs_is_skipped_played_at_plus_1d_month_").desc(), "id")
w_track_id_source = Window.partitionBy("track_id", "source")
w_source_by_running_ms_played = Window.partitionBy("source").orderBy("running_ms_played", "id").rowsBetween(-2, Window.currentRow)
w_listener_id_by_ms_played_vs = Window.partitionBy("listener_id").orderBy(F.col("ms_played_vs_is_skipped_played_at_plus_1d_month__3").desc(), "id").rowsBetween(-2, Window.currentRow)
w_source_track_artist_id = Window.partitionBy("source", "track_artist_id")

valid = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
        (F.col("played_at") + F.expr("INTERVAL 1 DAYS")).alias("played_at_plus_1d"),
    )
)

ranked = (
    valid
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        "played_at_label",
        "played_at_plus_1d",
        F.min("ms_played").over(w_is_skipped_played_at_plus_1d).alias("is_skipped_played_at_plus_1d_month_min_ms_played"),
        F.count(F.lit(1)).over(w_is_skipped_played_at_plus_1d).alias("rows_in_is_skipped_played_at_plus_1d_month"),
        F.min("ms_played").over(w_device_by_ms_played_running).alias("min_ms_played_so_far"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_is_skipped_played_at_plus_1d), 2).alias("ms_played_vs_is_skipped_played_at_plus_1d_month_"),
        F.sum("ms_played").over(w_device_by_ms_played_running).alias("running_ms_played"),
    )
)

source_stats = (
    ranked
    .groupBy("source")
    .agg(F.percentile_approx("ms_played_vs_is_skipped_played_at_plus_1d_month_", 0.5).alias("median_ms_played_vs_is_skipped_played_at_plus_1d"))
)

above_average = (
    ranked.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played_vs_is_skipped_played_at_plus_1d_month_") >= F.col("g.median_ms_played_vs_is_skipped_played_at_plus_1d"))
    .select(
        "s.*",
        "g.median_ms_played_vs_is_skipped_played_at_plus_1d",
        F.round(F.col("s.ms_played_vs_is_skipped_played_at_plus_1d_month_") - F.col("g.median_ms_played_vs_is_skipped_played_at_plus_1d"), 2).alias("ms_played_vs_is_skipped_played_at_plus_1d_month__2"),
    )
)

sequenced = (
    above_average
    .select(
        "*",
        F.size(F.collect_set("track_id").over(w_listener_id)).alias("distinct_track_id_in_listener_id"),
        F.round(F.col("running_ms_played") - F.avg("running_ms_played").over(w_listener_id), 2).alias("running_ms_played_vs_listener_id_avg"),
        F.round(F.avg("min_ms_played_so_far").over(w_listener_id), 2).alias("listener_id_avg_min_ms_played_so_far"),
        F.rank().over(w_all_by_rows_in_is_skipped_pl).alias("rows_in_is_skipped_played_at_plus_1d_month_rank"),
    )
)

sequenced_2 = (
    sequenced
    .withColumn("ms_played_vs_is_skipped_played_at_plus_1d_month__3", F.round(F.cume_dist().over(w_rows_in_is_skipped_played_at), 3))
    .withColumn("running_ms_played_vs_track_id_source_avg", F.round(F.col("running_ms_played") - F.avg("running_ms_played").over(w_track_id_source), 2))
)

with_parent = (
    sequenced_2.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.played_at_label",
        "s.is_skipped_played_at_plus_1d_month_min_ms_played",
        "s.min_ms_played_so_far",
        "s.ms_played_vs_is_skipped_played_at_plus_1d_month_",
        "s.running_ms_played",
        "s.median_ms_played_vs_is_skipped_played_at_plus_1d",
        "s.ms_played_vs_is_skipped_played_at_plus_1d_month__2",
        "s.rows_in_is_skipped_played_at_plus_1d_month_rank",
        "s.ms_played_vs_is_skipped_played_at_plus_1d_month__3",
        "s.running_ms_played_vs_track_id_source_avg",
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

with_trends = (
    with_parent
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "played_at_label",
        "is_skipped_played_at_plus_1d_month_min_ms_played",
        "min_ms_played_so_far",
        "running_ms_played",
        "median_ms_played_vs_is_skipped_played_at_plus_1d",
        "ms_played_vs_is_skipped_played_at_plus_1d_month__2",
        "ms_played_vs_is_skipped_played_at_plus_1d_month__3",
        "running_ms_played_vs_track_id_source_avg",
        "track_artist_id",
        F.max("running_ms_played").over(w_source_by_running_ms_played).alias("running_ms_played_max_3"),
        F.round(F.avg("running_ms_played").over(w_listener_id_by_ms_played_vs), 2).alias("running_ms_played_avg_3"),
        F.round(F.col("running_ms_played") - F.avg("running_ms_played").over(w_source_track_artist_id), 2).alias("running_ms_played_vs_source_track_artist_id_avg"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "played_at_label",
        "is_skipped_played_at_plus_1d_month_min_ms_played",
        "running_ms_played",
        "median_ms_played_vs_is_skipped_played_at_plus_1d",
        "running_ms_played_vs_track_id_source_avg",
        "track_artist_id",
        "running_ms_played_max_3",
        "running_ms_played_avg_3",
    )
)
