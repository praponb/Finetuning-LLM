from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_source = Window.partitionBy("listener_id", "source")
w_listener_signup_date_month_l = Window.partitionBy(F.date_trunc("month", "listener_signup_date"), "listener_id")
w_all_by_track_release_date_ru = Window.orderBy(F.col("track_release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_by_track_release_date = Window.partitionBy("device").orderBy(F.col("track_release_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_device_by_track_release_date_2 = Window.partitionBy("device").orderBy(F.col("track_release_date").desc(), "id")
w_all_by_track_release_date = Window.orderBy(F.col("track_release_date").desc(), "id")
w_all_by_track_release_date_mo = Window.orderBy(F.col("track_release_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_track_release_date_ti = Window.orderBy(F.col("track_release_date").desc())
w_is_skipped_played_at_month = Window.partitionBy("is_skipped", F.month("played_at"))
w_track_release_date_month = Window.partitionBy(F.date_trunc("month", "track_release_date"))
w_listener_id_by_listener_sign = Window.partitionBy("listener_id").orderBy(F.col("listener_signup_date").desc(), "id")
w_source_by_last_known_listene = Window.partitionBy("source").orderBy("last_known_listener_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_listener_sign_2 = Window.partitionBy("listener_id").orderBy(F.col("listener_signup_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_plays
    .filter(F.col("listener_id") >= 9)
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

enriched = (
    valid.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("t.release_date").alias("track_release_date"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.country").alias("listener_country"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "track_release_date",
        "listener_signup_date",
        F.count(F.lit(1)).over(w_listener_id_source).alias("rows_in_listener_id_source"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_signup_date_month_l), 2).alias("ms_played_vs_listener_signup_date_month_listener"),
        F.last("listener_signup_date", ignorenulls=True).over(w_all_by_track_release_date_ru).alias("last_known_listener_signup_date"),
        F.round(F.avg("ms_played").over(w_device_by_track_release_date), 2).alias("ms_played_avg_2"),
        F.datediff(F.col("track_release_date"), F.lag("track_release_date").over(w_device_by_track_release_date_2)).alias("days_since_prev_track_release_date"),
        F.ntile(5).over(w_all_by_track_release_date).alias("track_release_date_quintile"),
        F.ntile(3).over(w_device_by_track_release_date_2).alias("track_release_date_tercile"),
        F.sum("ms_played").over(w_all_by_track_release_date_mo).alias("ms_played_sum_2"),
        F.first("track_release_date", ignorenulls=True).over(w_all_by_track_release_date_ru).alias("first_known_track_release_date"),
        (F.col("ms_played") - F.lag("ms_played").over(w_device_by_track_release_date_2)).alias("ms_played_change"),
        F.round(F.cume_dist().over(w_all_by_track_release_date_ti), 3).alias("track_release_date_cume_dist"),
        F.sum("ms_played").over(w_is_skipped_played_at_month).alias("is_skipped_played_at_month_sum_ms_played"),
    )
)

scored = (
    ranked
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        "track_release_date",
        "listener_signup_date",
        "rows_in_listener_id_source",
        "ms_played_vs_listener_signup_date_month_listener",
        "last_known_listener_signup_date",
        "days_since_prev_track_release_date",
        "track_release_date_quintile",
        "track_release_date_tercile",
        "ms_played_sum_2",
        "first_known_track_release_date",
        "ms_played_change",
        "is_skipped_played_at_month_sum_ms_played",
        (F.col("ms_played_avg_2") + F.col("ms_played_change")).alias("ms_played_avg_2_plus_ms_played_change"),
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
        F.round(F.col("ms_played_avg_2") * 1.07, 2).alias("ms_played_avg_2_adj"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.round(F.avg("ms_played_change").over(w_track_release_date_month), 2).alias("track_release_date_month_avg_ms_played_change"),
        F.lag("track_release_date_tercile", 3, 0).over(w_listener_id_by_listener_sign).alias("prev3_track_release_date_tercile"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_release_date_month), F.lit(0)), 4).alias("ms_played_share_of_track_release_date_month"),
        F.sum("ms_played_avg_2_adj").over(w_source_by_last_known_listene).alias("running_ms_played_avg_2_adj"),
        F.first("rows_in_listener_id_source", ignorenulls=True).over(w_listener_id_by_listener_sign_2).alias("first_known_rows_in_listener_id_source"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        "track_release_date",
        "listener_signup_date",
        "rows_in_listener_id_source",
        "ms_played_vs_listener_signup_date_month_listener",
        "last_known_listener_signup_date",
        "track_release_date_quintile",
        "track_release_date_tercile",
        "ms_played_avg_2_plus_ms_played_change",
        "source_clean",
        "ms_played_avg_2_adj",
        "prev3_track_release_date_tercile",
        "ms_played_share_of_track_release_date_month",
        "running_ms_played_avg_2_adj",
        "first_known_rows_in_listener_id_source",
    )
    .orderBy("source_clean", "source")
)
