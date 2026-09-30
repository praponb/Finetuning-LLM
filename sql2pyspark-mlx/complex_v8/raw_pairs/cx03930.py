from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source")
w_device_by_played_at_prior_2 = Window.partitionBy("device").orderBy(F.col("played_at").desc(), "id").rowsBetween(-2, -1)
w_device_played_at_month = Window.partitionBy("device", F.month("played_at"))
w_device_track_id_by_source_ma = Window.partitionBy("device", "track_id").orderBy(F.col("source_max_ms_played").desc(), "id").rowsBetween(-6, Window.currentRow)
w_listener_id_track_id_by_play = Window.partitionBy("listener_id", "track_id").orderBy(F.col("played_at").desc())
w_listener_id_track_id_by_play_2 = Window.partitionBy("listener_id", "track_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_plays = (
    df_plays
    .filter(F.col("source") == "radio")
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

sequenced = (
    cleaned_plays
    .select(
        "*",
        F.round(F.avg("ms_played").over(w_source), 2).alias("source_avg_ms_played"),
        F.max("ms_played").over(w_source).alias("source_max_ms_played"),
        F.max("ms_played").over(w_device_by_played_at_prior_2).alias("ms_played_max_2_prior"),
    )
)

with_activity = (
    sequenced.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.track_id") <= 2), "left_anti")
)

sequenced_2 = (
    with_activity
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source_avg_ms_played",
        "ms_played_max_2_prior",
        F.size(F.collect_set("track_id").over(w_device_played_at_month)).alias("distinct_track_id_in_device_played_at_month"),
        F.round(F.avg("source_max_ms_played").over(w_device_track_id_by_source_ma), 2).alias("source_max_ms_played_avg_7"),
        F.round(F.cume_dist().over(w_listener_id_track_id_by_play), 3).alias("played_at_cume_dist"),
        F.last("source_max_ms_played").over(w_listener_id_track_id_by_play_2).alias("last_source_max_ms_played"),
        F.rank().over(w_listener_id_track_id_by_play).alias("played_at_rank"),
        F.sum("ms_played_max_2_prior").over(w_device_played_at_month).alias("device_played_at_month_sum_ms_played_max_2_prior"),
    )
)

result = (
    sequenced_2
    .groupingSets(
        [["distinct_track_id_in_device_played_at_month", "device"], ["distinct_track_id_in_device_played_at_month"]],
        "distinct_track_id_in_device_played_at_month", "device",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.stddev("distinct_track_id_in_device_played_at_month"), 2).alias("stddev_distinct_track_id_in_device_played_at_mon"),
        F.round(F.sum("source_max_ms_played_avg_7") / F.nullif(F.sum("device_played_at_month_sum_ms_played_max_2_prior"), F.lit(0)), 3).alias("source_max_ms_played_avg_7_to_device_played_at_m"),
    )
    .orderBy(
        F.col("distinct_track_id_in_device_played_at_month").asc_nulls_last(),
        F.col("device").asc_nulls_last(),
    )
)
