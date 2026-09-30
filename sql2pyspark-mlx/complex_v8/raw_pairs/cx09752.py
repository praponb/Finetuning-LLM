from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_all_by_listener_signup_date = Window.orderBy(F.col("listener_signup_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_listener_id = Window.partitionBy("source", "listener_id")
w_listener_signup_date_year_by = Window.partitionBy(F.year("listener_signup_date")).orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_month_by_listener = Window.partitionBy(F.month("played_at")).orderBy(F.col("listener_signup_date").desc(), "id").rowsBetween(-2, -1)
w_played_at_month_by_listener_2 = Window.partitionBy(F.month("played_at")).orderBy(F.col("listener_signup_date").desc(), "id")
w_listener_signup_date_year_by_2 = Window.partitionBy(F.year("listener_signup_date")).orderBy(F.col("ms_played").desc())
w_listener_id_distinct_listene = Window.partitionBy("listener_id", "distinct_listener_id_in_track_id")
w_listener_id_source_by_rows_i = Window.partitionBy("listener_id", "source").orderBy(F.col("rows_in_track_id").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_rows_in_track_id_full = Window.orderBy("rows_in_track_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_rows_in_track_id_ties = Window.orderBy("rows_in_track_id")
w_played_at_month_by_rows_in_t = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy("rows_in_track_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.date_trunc("hour", F.col("played_at")).alias("played_at_hour"),
    )
)

with_details = (
    base_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.device_slug",
        "s.played_at_hour",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.username").alias("listener_username"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.moods").alias("track_moods"),
    )
)

windowed = (
    with_details
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "source",
        "device_slug",
        "listener_signup_date",
        "listener_username",
        "track_release_date",
        "track_moods",
        F.size(F.collect_set("listener_id").over(w_track_id)).alias("distinct_listener_id_in_track_id"),
        F.max("ms_played").over(w_track_id).alias("track_id_max_ms_played"),
        F.nth_value("played_at", 2).over(w_all_by_listener_signup_date).alias("second_played_at"),
        F.round(F.avg("ms_played").over(w_track_id), 2).alias("track_id_avg_ms_played"),
        F.count(F.lit(1)).over(w_source_listener_id).alias("rows_in_source_listener_id"),
        F.max("ms_played").over(w_listener_signup_date_year_by).alias("max_ms_played_so_far"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.min("ms_played").over(w_source_listener_id).alias("source_listener_id_min_ms_played"),
        F.round(F.avg("ms_played").over(w_played_at_month_by_listener), 2).alias("ms_played_avg_2_prior"),
        F.round(F.cume_dist().over(w_played_at_month_by_listener_2), 3).alias("listener_signup_date_cume_dist"),
        F.rank().over(w_listener_signup_date_year_by_2).alias("ms_played_rank"),
        F.max("ms_played").over(w_source_listener_id).alias("source_listener_id_max_ms_played"),
        F.min("ms_played").over(w_track_id).alias("track_id_min_ms_played"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device_slug",
        "listener_username",
        "track_release_date",
        "rows_in_source_listener_id",
        "max_ms_played_so_far",
        "source_listener_id_min_ms_played",
        "source_listener_id_max_ms_played",
        F.round(F.avg("rows_in_track_id").over(w_listener_id_distinct_listene), 2).alias("listener_id_distinct_listener_id_in_track_id_avg"),
        F.sum("max_ms_played_so_far").over(w_listener_id_distinct_listene).alias("listener_id_distinct_listener_id_in_track_id_sum"),
        F.round(F.avg("track_id_min_ms_played").over(w_listener_id_distinct_listene), 2).alias("listener_id_distinct_listener_id_in_track_id_avg_2"),
        F.round(F.avg("track_id_min_ms_played").over(w_listener_id_source_by_rows_i), 2).alias("track_id_min_ms_played_avg_4"),
        F.first("track_release_date").over(w_all_by_rows_in_track_id_full).alias("first_track_release_date"),
        F.round(F.cume_dist().over(w_all_by_rows_in_track_id_ties), 3).alias("rows_in_track_id_cume_dist"),
        F.last("track_id_avg_ms_played").over(w_played_at_month_by_rows_in_t).alias("last_track_id_avg_ms_played"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device_slug",
        "listener_username",
        "track_release_date",
        "rows_in_source_listener_id",
        "max_ms_played_so_far",
        "source_listener_id_max_ms_played",
        "listener_id_distinct_listener_id_in_track_id_avg",
        "track_id_min_ms_played_avg_4",
        "rows_in_track_id_cume_dist",
        "last_track_id_avg_ms_played",
        F.substring(F.trim(F.col("device_slug")), 1, 3).alias("device_slug_prefix"),
    )
    .orderBy(
        F.col("track_id_min_ms_played_avg_4").asc(),
        F.col("max_ms_played_so_far").desc(),
        "id",
    )
    .limit(10)
)
