from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_group_by_played_at_da = Window.partitionBy("device_group").orderBy("played_at_date")
w_listener_id_device_group_by = Window.partitionBy("listener_id", "device_group").orderBy(F.col("ms_played").desc(), "id")
w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_listener_id = Window.partitionBy("listener_id")
w_device_group_by_played_at_da_2 = Window.partitionBy("device_group").orderBy("played_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_date_month_listene = Window.partitionBy(F.date_trunc("month", "played_at_date"), "listener_id")
w_listener_id_by_played_at_dat = Window.partitionBy("listener_id").orderBy("played_at_date", "id")
w_device_group_is_skipped_by_m = Window.partitionBy("device_group", "is_skipped").orderBy("ms_played", "id").rowsBetween(-3, -1)
w_device_group_is_skipped_by_m_2 = Window.partitionBy("device_group", "is_skipped").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played_full = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_plays = (
    df_plays
    .filter(F.col("ms_played") <= 301149)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.when(F.col("device").isin("phone", "speaker"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group"),
    )
)

with_windows = (
    cleaned_plays
    .select(
        "*",
        F.round(F.percent_rank().over(w_device_group_by_played_at_da), 3).alias("played_at_date_percent_rank"),
        F.lead("ms_played").over(w_listener_id_device_group_by).alias("next_ms_played"),
        F.size(F.collect_set("source").over(w_track_id_listener_id)).alias("distinct_source_in_track_id_listener_id"),
        F.lag("played_at_date", 2).over(w_all_by_ms_played).alias("prev2_played_at_date"),
        F.size(F.collect_set("track_id").over(w_listener_id)).alias("distinct_track_id_in_listener_id"),
        F.last("played_at_date", ignorenulls=True).over(w_device_group_by_played_at_da_2).alias("last_known_played_at_date"),
        F.count(F.lit(1)).over(w_played_at_date_month_listene).alias("rows_in_played_at_date_month_listener_id"),
        F.min("ms_played").over(w_track_id_listener_id).alias("track_id_listener_id_min_ms_played"),
        (F.col("ms_played") - F.lag("ms_played").over(w_listener_id_by_played_at_dat)).alias("ms_played_change"),
        F.sum("ms_played").over(w_device_group_is_skipped_by_m).alias("ms_played_sum_3_prior"),
        F.size(F.collect_set("track_id").over(w_played_at_date_month_listene)).alias("distinct_track_id_in_played_at_date_month_listen"),
        F.size(F.collect_set("device_group").over(w_track_id_listener_id)).alias("distinct_device_group_in_track_id_listener_id"),
        F.nth_value("played_at_date", 2).over(w_device_group_is_skipped_by_m_2).alias("second_played_at_date"),
        F.ntile(3).over(w_listener_id_by_played_at_dat).alias("played_at_date_tercile"),
        F.first("played_at_date").over(w_all_by_ms_played_full).alias("first_played_at_date"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "track_id",
        "ms_played",
        "is_skipped",
        "device_group",
        "played_at_date_percent_rank",
        "distinct_source_in_track_id_listener_id",
        "prev2_played_at_date",
        "distinct_track_id_in_listener_id",
        "last_known_played_at_date",
        "rows_in_played_at_date_month_listener_id",
        "track_id_listener_id_min_ms_played",
        "ms_played_change",
        "ms_played_sum_3_prior",
        "distinct_track_id_in_played_at_date_month_listen",
        "second_played_at_date",
        "first_played_at_date",
    )
)
