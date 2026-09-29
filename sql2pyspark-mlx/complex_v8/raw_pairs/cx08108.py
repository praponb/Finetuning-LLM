from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_month = Window.partitionBy(F.month("played_at"))
w_is_skipped_by_played_at_ties = Window.partitionBy("is_skipped").orderBy(F.col("played_at").desc())
w_device_track_id = Window.partitionBy("device", "track_id")
w_played_at_year_listener_id_b = Window.partitionBy(F.year("played_at"), "listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_ms_played_runn = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_year_listener_id_b_2 = Window.partitionBy(F.year("played_at"), "listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc_nulls_first(), "id")
w_track_id_device_by_listener = Window.partitionBy("track_id", "device").orderBy(F.col("listener_birth_year").desc(), "id")
w_all_by_running_count_2_runni = Window.orderBy(F.col("running_count_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_moving_6 = Window.orderBy("ms_played", "id").rowsBetween(-6, Window.currentRow)
w_all_by_ms_played_ties = Window.orderBy("ms_played")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_all_by_running_count_2 = Window.orderBy(F.col("running_count_2").desc(), "id")
w_listener_id_track_id = Window.partitionBy("listener_id", "track_id")

prepared = df_plays.filter(F.col("is_skipped"))

with_parent = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .filter(F.upper(F.trim(F.col("t.title"))).like("E%"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        F.col("t.title").alias("track_title"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

sequenced = (
    with_parent
    .select(
        "*",
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_played_at_month), 2).alias("ms_played_vs_played_at_month_avg"),
        F.size(F.collect_set("track_id").over(w_played_at_month)).alias("distinct_track_id_in_played_at_month"),
        F.round(F.percent_rank().over(w_is_skipped_by_played_at_ties), 3).alias("played_at_percent_rank"),
        F.round(F.col("listener_birth_year") / F.nullif(F.sum("listener_birth_year").over(w_device_track_id), F.lit(0)), 4).alias("listener_birth_year_share_of_device_track_id"),
        F.nth_value("played_at", 2).over(w_played_at_year_listener_id_b).alias("second_played_at"),
        F.count(F.lit(1)).over(w_is_skipped_by_ms_played_runn).alias("running_count"),
        F.round(F.col("listener_birth_year") - F.avg("listener_birth_year").over(w_played_at_month), 2).alias("listener_birth_year_vs_played_at_month_avg"),
        F.count(F.lit(1)).over(w_played_at_year_listener_id_b_2).alias("running_count_2"),
        F.max("listener_birth_year").over(w_device_track_id).alias("device_track_id_max_listener_birth_year"),
        F.dense_rank().over(w_is_skipped_by_played_at_ties).alias("played_at_dense_rank"),
        F.count(F.lit(1)).over(w_played_at_month).alias("rows_in_played_at_month"),
        F.rank().over(w_is_skipped_by_ms_played).alias("ms_played_rank"),
        F.lead("played_at").over(w_track_id_device_by_listener).alias("next_played_at"),
        F.size(F.collect_set("listener_id").over(w_device_track_id)).alias("distinct_listener_id_in_device_track_id"),
        F.sum("listener_birth_year").over(w_device_track_id).alias("device_track_id_sum_listener_birth_year"),
    )
)

windowed = (
    sequenced
    .select(
        "*",
        F.last("played_at_percent_rank", ignorenulls=True).over(w_all_by_running_count_2_runni).alias("last_known_played_at_percent_rank"),
        F.max("rows_in_played_at_month").over(w_all_by_ms_played_moving_6).alias("rows_in_played_at_month_max_7"),
        F.rank().over(w_all_by_ms_played_ties).alias("ms_played_rank_2"),
        (F.col("rows_in_played_at_month") - F.lag("rows_in_played_at_month").over(w_all_by_ms_played)).alias("rows_in_played_at_month_change"),
        (F.col("played_at_percent_rank") - F.lag("played_at_percent_rank").over(w_all_by_running_count_2)).alias("played_at_percent_rank_change"),
        F.count(F.lit(1)).over(w_listener_id_track_id).alias("rows_in_listener_id_track_id"),
        F.lead("second_played_at").over(w_all_by_ms_played).alias("next_second_played_at"),
    )
)

result = (
    windowed
    .filter(F.col("ms_played_vs_played_at_month_avg") < 280000.0)
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "track_title",
        "listener_birth_year",
        "ms_played_vs_played_at_month_avg",
        "distinct_track_id_in_played_at_month",
        "played_at_percent_rank",
        "listener_birth_year_share_of_device_track_id",
        "listener_birth_year_vs_played_at_month_avg",
        "running_count_2",
        "device_track_id_max_listener_birth_year",
        "played_at_dense_rank",
        "next_played_at",
        "distinct_listener_id_in_device_track_id",
        "last_known_played_at_percent_rank",
        "rows_in_played_at_month_max_7",
        "ms_played_rank_2",
        "rows_in_played_at_month_change",
        "played_at_percent_rank_change",
        "rows_in_listener_id_track_id",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.date_trunc("quarter", F.col("played_at")).alias("played_at_quarter"),
    )
    .orderBy(
        "distinct_listener_id_in_device_track_id",
        F.col("listener_birth_year_share_of_device_track_id").asc(),
        "id",
    )
    .limit(20)
)
