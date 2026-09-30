from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_is_skipped = Window.partitionBy("track_id", "is_skipped")
w_track_id_by_played_at_week_t = Window.partitionBy("track_id").orderBy(F.col("played_at_week").desc())
w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")
w_device_by_ms_played = Window.partitionBy("device").orderBy("ms_played", "id")
w_device_listener_country_by_p = Window.partitionBy("device", "listener_country").orderBy(F.col("played_at_week").desc(), "id")
w_device_listener_country_by_p_2 = Window.partitionBy("device", "listener_country").orderBy(F.col("played_at_week").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy("ms_played", "id")
w_all_by_played_at_week = Window.orderBy("played_at_week", "id")
w_track_id_by_played_at_week = Window.partitionBy("track_id").orderBy(F.col("played_at_week").desc(), "id")
w_all_by_played_at_week_prior = Window.orderBy("played_at_week", "id").rowsBetween(-6, -1)
w_device_by_ms_played_moving_6 = Window.partitionBy("device").orderBy("ms_played", "id").rowsBetween(-6, Window.currentRow)
w_played_at_week_quintile_by_m = Window.partitionBy("played_at_week_quintile").orderBy(F.col("ms_played").desc(), "id")
w_is_skipped = Window.partitionBy("is_skipped")
w_second_played_at_week_month = Window.partitionBy(F.date_trunc("month", "second_played_at_week"))

base = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
        F.date_trunc("week", F.col("played_at")).alias("played_at_week"),
    )
)

joined = (
    base.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.plan").isNotNull())
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.played_at_label",
        "s.played_at_week",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.country").alias("listener_country"),
    )
)

windowed = (
    joined
    .select(
        "*",
        F.count(F.lit(1)).over(w_track_id_is_skipped).alias("rows_in_track_id_is_skipped"),
        F.round(F.percent_rank().over(w_track_id_by_played_at_week_t), 3).alias("played_at_week_percent_rank"),
        F.max("ms_played").over(w_track_id_listener_id).alias("track_id_listener_id_max_ms_played"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id_listener_id), F.lit(0)), 4).alias("ms_played_share_of_track_id_listener_id"),
        F.lag("ms_played", 2).over(w_device_by_ms_played).alias("prev2_ms_played"),
        F.dense_rank().over(w_device_listener_country_by_p).alias("played_at_week_dense_rank"),
        F.nth_value("played_at_week", 2).over(w_device_listener_country_by_p_2).alias("second_played_at_week"),
        (F.col("ms_played") - F.lag("ms_played").over(w_listener_id_by_ms_played)).alias("ms_played_change"),
        F.size(F.collect_set("listener_plan").over(w_track_id_listener_id)).alias("distinct_listener_plan_in_track_id_listener_id"),
        F.ntile(5).over(w_all_by_played_at_week).alias("played_at_week_quintile"),
        F.lag("ms_played", 3).over(w_track_id_by_played_at_week).alias("prev3_ms_played"),
        F.round(F.avg("ms_played").over(w_track_id_is_skipped), 2).alias("track_id_is_skipped_avg_ms_played"),
        F.round(F.avg("ms_played").over(w_all_by_played_at_week_prior), 2).alias("ms_played_avg_6_prior"),
        F.sum("ms_played").over(w_device_by_ms_played_moving_6).alias("ms_played_sum_7"),
    )
)

metrics = (
    windowed
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "played_at_week",
        "listener_plan",
        "rows_in_track_id_is_skipped",
        "track_id_listener_id_max_ms_played",
        "ms_played_share_of_track_id_listener_id",
        "prev2_ms_played",
        "played_at_week_dense_rank",
        "ms_played_change",
        "distinct_listener_plan_in_track_id_listener_id",
        "played_at_week_quintile",
        "prev3_ms_played",
        "track_id_is_skipped_avg_ms_played",
        "ms_played_avg_6_prior",
        "ms_played_sum_7",
        F.lag("rows_in_track_id_is_skipped", 1, 0).over(w_played_at_week_quintile_by_m).alias("prev_rows_in_track_id_is_skipped"),
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.count(F.lit(1)).over(w_second_played_at_week_month).alias("rows_in_second_played_at_week_month"),
    )
)

result = (
    metrics
    .filter(F.col("ms_played_sum_7") >= 244000)
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "listener_plan",
        "rows_in_track_id_is_skipped",
        "track_id_listener_id_max_ms_played",
        "prev2_ms_played",
        "ms_played_change",
        "distinct_listener_plan_in_track_id_listener_id",
        "played_at_week_quintile",
        "prev3_ms_played",
        "track_id_is_skipped_avg_ms_played",
        "ms_played_sum_7",
        "rows_in_second_played_at_week_month",
        F.greatest(F.col("prev3_ms_played"), F.col("played_at_week_quintile")).alias("greatest_prev3_ms_played_played_at_week_quintile"),
        F.concat(F.col("listener_plan"), F.lit("#"), F.col("track_id").cast("string")).alias("listener_plan_ref"),
    )
    .orderBy("ms_played_sum_7", F.col("track_id").desc())
)
