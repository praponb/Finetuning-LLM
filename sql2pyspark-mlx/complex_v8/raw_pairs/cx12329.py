from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_device_track_id = Window.partitionBy("device", "track_id")
w_device_listener_id_by_ms_pla = Window.partitionBy("device", "listener_id").orderBy("ms_played")
w_device_is_skipped_by_ms_play = Window.partitionBy("device", "is_skipped").orderBy(F.col("ms_played").desc(), "id")
w_device_is_skipped_by_ms_play_2 = Window.partitionBy("device", "is_skipped").orderBy(F.col("ms_played").desc())
w_all_by_ms_played_cume_dist = Window.orderBy("ms_played_cume_dist", "id")
w_listener_id_track_id = Window.partitionBy("listener_id", "track_id")
w_first_played_at_to_date_play = Window.partitionBy(F.to_date("first_played_at"), F.to_date("played_at_month")).orderBy(F.col("first_played_at").asc_nulls_last())
w_first_played_at_to_date_play_2 = Window.partitionBy(F.to_date("first_played_at"), F.to_date("played_at_month")).orderBy(F.col("first_played_at").asc_nulls_last(), "track_id", "played_at_month", "device").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_played_at_to_date_play_3 = Window.partitionBy(F.to_date("first_played_at"), F.to_date("played_at_month")).orderBy(F.col("first_played_at").asc_nulls_last(), "track_id", "played_at_month", "device").rowsBetween(-2, -1)
w_first_played_at_to_date_play_4 = Window.partitionBy(F.to_date("first_played_at"), F.to_date("played_at_month")).orderBy(F.col("first_played_at").asc_nulls_last(), "track_id", "played_at_month", "device")

base_plays = (
    df_plays
    .filter((F.col("source").isin("playlist", "search")) & (~F.col("is_skipped")))
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

with_trends = (
    base_plays
    .select(
        "*",
        F.max("ms_played").over(w_track_id).alias("track_id_max_ms_played"),
        F.row_number().over(w_all_by_ms_played).alias("seq_no"),
        F.lag("played_at").over(w_all_by_ms_played).alias("prev_played_at"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device_track_id), F.lit(0)), 4).alias("ms_played_share_of_device_track_id"),
        F.sum("ms_played").over(w_device_track_id).alias("device_track_id_sum_ms_played"),
        F.round(F.cume_dist().over(w_device_listener_id_by_ms_pla), 3).alias("ms_played_cume_dist"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id), 2).alias("ms_played_vs_track_id_avg"),
        F.lag("ms_played").over(w_device_is_skipped_by_ms_play).alias("prev_ms_played"),
        F.rank().over(w_device_is_skipped_by_ms_play_2).alias("ms_played_rank"),
    )
)

top_per_group = with_trends.filter(F.col("ms_played_rank") <= 3)

with_windows = (
    top_per_group
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_ms_played_cume_dist), 3).alias("ms_played_cume_dist_cume_dist"),
        F.max("ms_played_vs_track_id_avg").over(w_listener_id_track_id).alias("listener_id_track_id_max_ms_played_vs_track_id_a"),
        F.count(F.lit(1)).over(w_listener_id_track_id).alias("rows_in_listener_id_track_id"),
        F.round(F.avg("ms_played_cume_dist").over(w_listener_id_track_id), 2).alias("listener_id_track_id_avg_ms_played_cume_dist"),
    )
)

totals = (
    with_windows
    .filter(F.col("ms_played_cume_dist") <= 0.8)
    .groupBy(
        "track_id",
        F.date_trunc("month", "played_at").alias("played_at_month"),
        "device",
    )
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.min("prev_played_at").alias("first_prev_played_at"),
        F.round(100 * F.count(F.when(F.col("is_skipped"), 1)) / F.count("*"), 1).alias("pct_is_skipped_true"),
        F.sum(F.when(F.col("device") != "desktop", F.col("listener_id_track_id_max_ms_played_vs_track_id_a")).otherwise(0)).alias("listener_id_track_id_max_ms_played_vs_track_id_a_2"),
        F.max("seq_no").alias("max_seq_no_check"),
    )
    .filter(F.col("max_seq_no_check") > 4)
    .drop("max_seq_no_check")
)

ranked = (
    totals
    .select(
        "track_id",
        "played_at_month",
        "device",
        "first_played_at",
        "pct_is_skipped_true",
        "listener_id_track_id_max_ms_played_vs_track_id_a_2",
        F.round(F.percent_rank().over(w_first_played_at_to_date_play), 3).alias("first_played_at_percent_rank"),
        F.count(F.lit(1)).over(w_first_played_at_to_date_play_2).alias("running_count"),
        F.max("pct_is_skipped_true").over(w_first_played_at_to_date_play_3).alias("pct_is_skipped_true_max_2_prior"),
        F.round(F.cume_dist().over(w_first_played_at_to_date_play_4), 3).alias("first_played_at_cume_dist"),
    )
)

result = (
    ranked
    .select(
        "track_id",
        "played_at_month",
        "first_played_at",
        "pct_is_skipped_true",
        "listener_id_track_id_max_ms_played_vs_track_id_a_2",
        "first_played_at_percent_rank",
        "running_count",
        "pct_is_skipped_true_max_2_prior",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.date_format(F.col("first_played_at"), "yyyy-MM-dd").alias("first_played_at_label"),
    )
    .orderBy(F.col("device_len").asc())
)
