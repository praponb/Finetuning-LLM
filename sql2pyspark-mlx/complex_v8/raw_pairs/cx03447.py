from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_played_at_prior = Window.partitionBy("track_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(-2, -1)
w_all_by_source_len = Window.orderBy("source_len", "id")
w_track_id_by_played_at = Window.partitionBy("track_id").orderBy(F.col("played_at").desc(), "id")
w_source_len_listener_id = Window.partitionBy("source_len", "listener_id")
w_track_id_source = Window.partitionBy("track_id", "source")
w_source_len_by_source_len_ful = Window.partitionBy("source_len").orderBy(F.col("source_len").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_prior_3 = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(-3, -1)
w_track_id_by_played_at_runnin = Window.partitionBy("track_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

with_details_listener = (
    valid.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source",
        "s.days_since_played_at",
        "s.source_len",
        F.col("l.username").alias("listener_username"),
    )
)

with_windows = (
    with_details_listener
    .select(
        "id",
        "played_at",
        "device",
        "source",
        "days_since_played_at",
        "source_len",
        "listener_username",
        F.sum("days_since_played_at").over(w_track_id_by_played_at_prior).alias("days_since_played_at_sum_2_prior"),
        F.lead("source_len", 3, 0).over(w_all_by_source_len).alias("next3_source_len"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_track_id_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.round(F.col("source_len") - F.avg("source_len").over(w_source_len_listener_id), 2).alias("source_len_vs_source_len_listener_id_avg"),
        F.count(F.lit(1)).over(w_track_id_source).alias("rows_in_track_id_source"),
        F.nth_value("source_len", 2).over(w_source_len_by_source_len_ful).alias("second_source_len"),
        F.round(F.avg("source_len").over(w_all_by_played_at_prior_3), 2).alias("source_len_avg_3_prior"),
        F.round(F.avg("source_len").over(w_source_len_listener_id), 2).alias("source_len_listener_id_avg_source_len"),
        F.count(F.lit(1)).over(w_track_id_by_played_at_runnin).alias("running_count"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "played_at",
        "device",
        "source",
        "days_since_played_at",
        "listener_username",
        "days_since_played_at_sum_2_prior",
        "next3_source_len",
        "source_len_vs_source_len_listener_id_avg",
        "second_source_len",
        "source_len_listener_id_avg_source_len",
        F.coalesce(F.col("days_since_played_at"), F.lit(0)).alias("days_since_played_at_filled"),
    )
)
