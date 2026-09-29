from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_ms_played_full = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_month = Window.partitionBy(F.date_trunc("month", "played_at"))
w_is_skipped_by_ms_played_prio = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-1, -1)
w_listener_id_is_skipped_by_ms = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("ms_played").desc(), "id")
w_played_at_year_by_ms_played = Window.partitionBy(F.year("played_at")).orderBy("ms_played", "id")
w_is_skipped_by_ms_played_prio_2 = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-3, -1)
w_listener_id_is_skipped_by_ms_2 = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device = Window.partitionBy("device")

base_plays = df_plays

with_windows = (
    base_plays
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.nth_value("played_at", 2).over(w_is_skipped_by_ms_played_full).alias("second_played_at"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_played_at_month), 2).alias("ms_played_vs_played_at_month_avg"),
        F.round(F.avg("ms_played").over(w_played_at_month), 2).alias("played_at_month_avg_ms_played"),
        F.sum("ms_played").over(w_is_skipped_by_ms_played_prio).alias("ms_played_sum_1_prior"),
        F.rank().over(w_listener_id_is_skipped_by_ms).alias("ms_played_rank"),
        F.lead("ms_played").over(w_played_at_year_by_ms_played).alias("next_ms_played"),
        F.round(F.percent_rank().over(w_listener_id_is_skipped_by_ms), 3).alias("ms_played_percent_rank"),
        F.round(F.avg("ms_played").over(w_is_skipped_by_ms_played_prio_2), 2).alias("ms_played_avg_3_prior"),
        F.min("ms_played").over(w_listener_id_is_skipped_by_ms_2).alias("min_ms_played_so_far"),
        F.size(F.collect_set("listener_id").over(w_device)).alias("distinct_listener_id_in_device"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_played_at_year_by_ms_played))) / 60, 1).alias("minutes_since_prev_played_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "second_played_at",
        "ms_played_vs_played_at_month_avg",
        "played_at_month_avg_ms_played",
        "ms_played_rank",
        "ms_played_percent_rank",
        "ms_played_avg_3_prior",
        "min_ms_played_so_far",
        "minutes_since_prev_played_at",
        F.concat_ws(": ", F.col("device"), F.col("source")).alias("device_source_label"),
    )
    .orderBy(F.col("ms_played_avg_3_prior").asc())
)
