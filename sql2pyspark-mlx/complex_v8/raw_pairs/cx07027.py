from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_device = Window.partitionBy("track_id", "device")
w_listener_plan_device_by_ms_p = Window.partitionBy("listener_plan", "device").orderBy("ms_played", "id")
w_track_id = Window.partitionBy("track_id")
w_track_id_track_is_explicit_b = Window.partitionBy("track_id", "track_is_explicit").orderBy("ms_played", "id")
w_is_skipped_by_track_release = Window.partitionBy("is_skipped").orderBy("track_release_date", "id")
w_listener_id_by_ms_played_tie = Window.partitionBy("listener_id").orderBy("ms_played")
w_listener_id_by_ms_played_pri = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(-6, -1)
w_is_skipped_by_track_release_2 = Window.partitionBy("is_skipped").orderBy("track_release_date", "id").rowsBetween(-6, -1)

scoped = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.abs(F.col("ms_played") - 168782).alias("ms_played_distance"),
    )
)

enriched = (
    scoped.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.played_at_date",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.username").alias("listener_username"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "track_id",
        "track_duration_sec",
        "track_is_explicit",
        "listener_plan",
        "listener_username",
        F.round(F.col("track_duration_sec") - F.avg("track_duration_sec").over(w_track_id_device), 2).alias("track_duration_sec_vs_track_id_device_avg"),
        F.round(F.cume_dist().over(w_listener_plan_device_by_ms_p), 3).alias("ms_played_cume_dist"),
        F.round(F.col("track_duration_sec") / F.nullif(F.sum("track_duration_sec").over(w_track_id), F.lit(0)), 4).alias("track_duration_sec_share_of_track_id"),
        F.round(F.percent_rank().over(w_track_id_track_is_explicit_b), 3).alias("ms_played_percent_rank"),
        F.round(F.avg("track_duration_sec").over(w_track_id_device), 2).alias("track_id_device_avg_track_duration_sec"),
        F.lag("track_duration_sec").over(w_is_skipped_by_track_release).alias("prev_track_duration_sec"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id), 2).alias("ms_played_vs_track_id_avg"),
        F.size(F.collect_set("device").over(w_track_id)).alias("distinct_device_in_track_id"),
        F.dense_rank().over(w_listener_id_by_ms_played_tie).alias("ms_played_dense_rank"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_device), 2).alias("ms_played_vs_track_id_device_avg"),
        F.ntile(5).over(w_is_skipped_by_track_release).alias("track_release_date_quintile"),
        F.round(F.avg("ms_played").over(w_track_id_device), 2).alias("track_id_device_avg_ms_played"),
        F.max("ms_played").over(w_listener_id_by_ms_played_pri).alias("ms_played_max_6_prior"),
        F.round(F.avg("ms_played").over(w_is_skipped_by_track_release_2), 2).alias("ms_played_avg_6_prior"),
    )
)

enriched_2 = (
    ranked
    .select(
        "*",
        F.substring(F.trim(F.col("listener_plan")), 1, 4).alias("listener_plan_prefix"),
        F.concat(F.col("listener_username"), F.lit("#"), F.col("prev_track_duration_sec").cast("string")).alias("listener_username_ref"),
        F.round(F.col("ms_played_percent_rank") / F.nullif(F.col("prev_track_duration_sec"), F.lit(0)), 3).alias("ms_played_percent_rank_per_prev_track_duration_s"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "track_id",
        "track_duration_sec",
        "track_is_explicit",
        "listener_username",
        "ms_played_percent_rank",
        "track_id_device_avg_track_duration_sec",
        "prev_track_duration_sec",
        "rows_in_track_id",
        "ms_played_vs_track_id_avg",
        "distinct_device_in_track_id",
        "ms_played_dense_rank",
        "ms_played_vs_track_id_device_avg",
        "track_release_date_quintile",
        "track_id_device_avg_ms_played",
        "listener_plan_prefix",
        "listener_username_ref",
        "ms_played_percent_rank_per_prev_track_duration_s",
    )
    .orderBy(F.col("prev_track_duration_sec").desc())
)
