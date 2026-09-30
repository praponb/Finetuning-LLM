from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at = Window.orderBy(F.col("played_at").asc_nulls_last(), "id")
w_all_by_ms_played_moving_1 = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(-1, Window.currentRow)
w_track_id_source_by_ms_played = Window.partitionBy("track_id", "source").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_played_at_month = Window.partitionBy("device", F.month("played_at"))
w_listener_id = Window.partitionBy("listener_id")
w_all_by_played_at_running = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_all_by_ms_played_2 = Window.orderBy(F.col("ms_played").desc(), "id")
w_track_id_source_by_ms_played_2 = Window.partitionBy("track_id", "source").orderBy("ms_played", "id").rowsBetween(-2, -1)
w_track_id_device = Window.partitionBy("track_id", "device")
w_all_by_ms_played_ties = Window.orderBy("ms_played")

prepared = (
    df_plays
    .filter((F.col("listener_id") >= 4) & (F.col("ms_played").between(92277, 324058)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
    )
)

ranked = (
    prepared
    .select(
        "*",
        F.lead("played_at").over(w_all_by_played_at).alias("next_played_at"),
        F.sum("ms_played").over(w_all_by_ms_played_moving_1).alias("ms_played_sum_2"),
        F.nth_value("ms_played", 2).over(w_track_id_source_by_ms_played).alias("second_ms_played"),
        F.sum("ms_played").over(w_device_played_at_month).alias("device_played_at_month_sum_ms_played"),
        F.round(F.avg("ms_played").over(w_device_played_at_month), 2).alias("device_played_at_month_avg_ms_played"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id), F.lit(0)), 4).alias("ms_played_share_of_listener_id"),
        F.sum("ms_played").over(w_all_by_played_at_running).alias("running_ms_played"),
        F.ntile(3).over(w_all_by_ms_played).alias("ms_played_tercile"),
        F.row_number().over(w_all_by_ms_played_2).alias("ms_played_seq"),
        F.max("ms_played").over(w_track_id_source_by_ms_played_2).alias("ms_played_max_2_prior"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device_played_at_month), 2).alias("ms_played_vs_device_played_at_month_avg"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_all_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id_device), F.lit(0)), 4).alias("ms_played_share_of_track_id_device"),
        F.rank().over(w_all_by_ms_played_ties).alias("ms_played_rank"),
    )
)

summary = (
    ranked
    .groupBy(
        "ms_played_rank",
        F.date_trunc("month", "next_played_at").alias("next_played_at_month"),
        "ms_played_seq",
    )
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
    )
)

result = (
    summary
    .filter(F.col("ms_played_rank") < 8)
    .select("ms_played_rank", "next_played_at_month", "ms_played_seq")
    .orderBy("ms_played_rank", F.col("ms_played_seq").desc())
)
