from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_track_bpm_full = Window.partitionBy("is_skipped").orderBy("track_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_listener_signup_date = Window.orderBy(F.col("listener_signup_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_is_skipped_by_playe = Window.partitionBy("track_id", "is_skipped").orderBy("played_at", "id")
w_listener_signup_date_year_is = Window.partitionBy(F.year("listener_signup_date"), "is_skipped").orderBy(F.col("track_bpm").asc_nulls_last(), "id")
w_is_skipped = Window.partitionBy("is_skipped")
w_track_id_is_skipped_by_playe_2 = Window.partitionBy("track_id", "is_skipped").orderBy("played_at", "id").rowsBetween(-1, Window.currentRow)
w_played_at_year_track_id_by_p = Window.partitionBy(F.year("played_at"), "track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_signup_date_month = Window.partitionBy(F.month("listener_signup_date"))
w_track_id_is_skipped_by_playe_3 = Window.partitionBy("track_id", "is_skipped").orderBy("played_at", "id").rowsBetween(-2, Window.currentRow)
w_device_by_listener_signup_da = Window.partitionBy("device").orderBy("listener_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_signup_date_month_b = Window.partitionBy(F.month("listener_signup_date")).orderBy("listener_signup_date", "id")
w_all_by_distinct_listener_id = Window.orderBy(F.col("distinct_listener_id_in_listener_signup_date_mon").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_plays = df_plays.filter((F.col("track_id").isNotNull()) & (~F.col("is_skipped")))

with_parent = (
    filtered_plays.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (~F.col("l.plan").isin("student", "free")), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

windowed = (
    with_parent
    .select(
        "id",
        "played_at",
        "ms_played",
        "device",
        "listener_signup_date",
        "track_moods",
        "track_bpm",
        F.first("listener_signup_date").over(w_is_skipped_by_track_bpm_full).alias("first_listener_signup_date"),
        F.count(F.lit(1)).over(w_all_by_listener_signup_date).alias("running_count"),
        F.ntile(4).over(w_track_id_is_skipped_by_playe).alias("played_at_quartile"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_listener_signup_date_year_is))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.round(F.avg("ms_played").over(w_track_id_is_skipped_by_playe_2), 2).alias("ms_played_avg_2"),
        F.row_number().over(w_listener_signup_date_year_is).alias("seq_no"),
        F.sum("track_bpm").over(w_played_at_year_track_id_by_p).alias("running_track_bpm"),
        F.round(F.percent_rank().over(w_track_id_is_skipped_by_playe), 3).alias("played_at_percent_rank"),
        F.min("ms_played").over(w_is_skipped).alias("is_skipped_min_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_signup_date_month), 2).alias("ms_played_vs_listener_signup_date_month_avg"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_signup_date_month), F.lit(0)), 4).alias("ms_played_share_of_listener_signup_date_month"),
        F.sum("ms_played").over(w_track_id_is_skipped_by_playe_3).alias("ms_played_sum_3"),
        F.size(F.collect_set("listener_id").over(w_listener_signup_date_month)).alias("distinct_listener_id_in_listener_signup_date_mon"),
    )
)

first_rows = windowed.filter(F.col("seq_no") <= 2)

ranked = (
    first_rows
    .select(
        "id",
        "played_at",
        "device",
        "track_moods",
        "first_listener_signup_date",
        "running_count",
        "played_at_quartile",
        "minutes_since_prev_played_at",
        "rows_in_is_skipped",
        "ms_played_avg_2",
        "seq_no",
        "is_skipped_min_ms_played",
        "ms_played_vs_listener_signup_date_month_avg",
        "distinct_listener_id_in_listener_signup_date_mon",
        F.first("ms_played", ignorenulls=True).over(w_device_by_listener_signup_da).alias("first_known_ms_played"),
        F.lag("listener_signup_date").over(w_listener_signup_date_month_b).alias("prev_listener_signup_date"),
        F.nth_value("running_count", 2).over(w_all_by_distinct_listener_id).alias("second_running_count"),
    )
)

per_group = (
    ranked
    .groupBy(F.to_date("played_at").alias("played_at_day"))
    .agg(
        F.sum(F.when(F.col("device") == "phone", 1).otherwise(0)).alias("n_device_phone"),
        F.count("*").alias("row_count"),
        F.sum("played_at_quartile").alias("total_played_at_quartile"),
        F.percentile_approx("played_at_quartile", 0.5).alias("median_played_at_quartile"),
        F.sum("minutes_since_prev_played_at").alias("total_minutes_since_prev_played_at"),
    )
)

result = (
    per_group
    .filter(F.col("total_played_at_quartile") <= 45)
    .select(
        "played_at_day",
        "n_device_phone",
        "row_count",
        "total_played_at_quartile",
        "median_played_at_quartile",
        (F.col("total_played_at_quartile") + F.col("n_device_phone")).alias("total_played_at_quartile_plus_n_device_phone"),
    )
    .orderBy("median_played_at_quartile")
)
