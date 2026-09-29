from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device")
w_played_at_year_by_played_at = Window.partitionBy(F.year("played_at")).orderBy(F.col("played_at").asc_nulls_last(), "id")
w_track_is_explicit_by_played = Window.partitionBy("track_is_explicit").orderBy("played_at", "id")
w_all_by_track_duration_sec_ti = Window.orderBy("track_duration_sec")
w_all_by_track_duration_sec_fu = Window.orderBy("track_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_is_explicit_by_played_2 = Window.partitionBy("track_is_explicit").orderBy("played_at", "id").rowsBetween(-2, Window.currentRow)
w_track_is_explicit_by_last_pl = Window.partitionBy("track_is_explicit").orderBy("last_played_at", "id")
w_all_by_played_at_dense_rank = Window.orderBy(F.col("played_at_dense_rank").desc(), "id")
w_all_by_played_at_dense_rank_2 = Window.orderBy(F.col("played_at_dense_rank").desc(), "id").rowsBetween(-3, Window.currentRow)
w_device_source = Window.partitionBy("device", "source")

cleaned_plays = df_plays

joined = (
    cleaned_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.title").alias("track_title"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

with_trends = (
    joined
    .select(
        "id",
        "played_at",
        "device",
        "source",
        "track_is_explicit",
        "track_duration_sec",
        F.min("track_duration_sec").over(w_device).alias("device_min_track_duration_sec"),
        F.dense_rank().over(w_played_at_year_by_played_at).alias("played_at_dense_rank"),
        F.size(F.collect_set("listener_id").over(w_device)).alias("distinct_listener_id_in_device"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.lag("track_duration_sec", 2, 0).over(w_track_is_explicit_by_played).alias("prev2_track_duration_sec"),
        F.round(F.percent_rank().over(w_all_by_track_duration_sec_ti), 3).alias("track_duration_sec_percent_rank"),
        F.lag("played_at").over(w_played_at_year_by_played_at).alias("prev_played_at"),
        F.last("played_at").over(w_all_by_track_duration_sec_fu).alias("last_played_at"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_played_at_year_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.sum("track_duration_sec").over(w_track_is_explicit_by_played_2).alias("track_duration_sec_sum_3"),
        F.nth_value("track_duration_sec", 2).over(w_all_by_track_duration_sec_fu).alias("second_track_duration_sec"),
        F.lag("track_duration_sec", 3).over(w_played_at_year_by_played_at).alias("prev3_track_duration_sec"),
        F.lead("track_duration_sec", 1, 0).over(w_played_at_year_by_played_at).alias("next_track_duration_sec"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        F.round((F.unix_timestamp("prev_played_at") - F.unix_timestamp(F.lag("prev_played_at").over(w_track_is_explicit_by_last_pl))) / 60, 1).alias("minutes_since_prev_prev_played_at"),
        F.ntile(5).over(w_all_by_played_at_dense_rank).alias("played_at_dense_rank_quintile"),
        F.round(F.avg("minutes_since_prev_played_at").over(w_all_by_played_at_dense_rank_2), 2).alias("minutes_since_prev_played_at_avg_4"),
        F.round(F.col("next_track_duration_sec") - F.avg("next_track_duration_sec").over(w_device_source), 2).alias("next_track_duration_sec_vs_device_source_avg"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "source",
        "track_is_explicit",
        "track_duration_sec",
        "played_at_dense_rank",
        "distinct_listener_id_in_device",
        "prev2_track_duration_sec",
        "prev_played_at",
        "last_played_at",
        "minutes_since_prev_played_at",
        "track_duration_sec_sum_3",
        "second_track_duration_sec",
        "next_track_duration_sec",
        "minutes_since_prev_prev_played_at",
        "played_at_dense_rank_quintile",
        "next_track_duration_sec_vs_device_source_avg",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.abs(F.col("played_at_dense_rank") - 4).alias("played_at_dense_rank_distance"),
    )
    .orderBy(
        F.col("minutes_since_prev_prev_played_at").desc(),
        "played_at_dense_rank_quintile",
    )
)
