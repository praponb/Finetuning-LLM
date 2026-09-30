from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_listener_id = Window.partitionBy("device", "listener_id")
w_listener_plan_source_by_trac = Window.partitionBy("listener_plan", "source").orderBy(F.col("track_release_date").desc(), "id")
w_listener_plan_source_by_trac_2 = Window.partitionBy("listener_plan", "source").orderBy(F.col("track_release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_listener_signup_date = Window.partitionBy("source", F.month("listener_signup_date")).orderBy(F.col("track_bpm").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_listener_signup_date = Window.orderBy(F.col("listener_signup_date").desc(), "id")
w_source_listener_signup_date_2 = Window.partitionBy("source", F.month("listener_signup_date")).orderBy(F.col("track_bpm").asc_nulls_last())
w_track_release_date_year_devi = Window.partitionBy(F.year("track_release_date"), "device").orderBy("track_release_date_cume_dist", "id").rowsBetween(-3, Window.currentRow)
w_track_release_date_year_devi_2 = Window.partitionBy(F.year("track_release_date"), "device").orderBy("track_release_date_cume_dist", "id")
w_track_release_date_year_devi_3 = Window.partitionBy(F.year("track_release_date"), "device").orderBy("track_release_date_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_plays = (
    df_plays
    .filter((F.col("source") == "search") & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
    )
)

with_details_listener = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

windowed = (
    with_details_listener
    .select(
        "*",
        F.size(F.collect_set("source").over(w_device_listener_id)).alias("distinct_source_in_device_listener_id"),
        F.round(F.cume_dist().over(w_listener_plan_source_by_trac), 3).alias("track_release_date_cume_dist"),
        F.max("track_bpm").over(w_listener_plan_source_by_trac_2).alias("max_track_bpm_so_far"),
        F.round(F.avg("track_bpm").over(w_source_listener_signup_date), 2).alias("track_bpm_avg_3"),
        F.max("track_bpm").over(w_device_listener_id).alias("device_listener_id_max_track_bpm"),
        F.size(F.collect_set("listener_plan").over(w_device_listener_id)).alias("distinct_listener_plan_in_device_listener_id"),
        F.round(F.col("track_bpm") - F.avg("track_bpm").over(w_device_listener_id), 2).alias("track_bpm_vs_device_listener_id_avg"),
        F.round(F.cume_dist().over(w_all_by_listener_signup_date), 3).alias("listener_signup_date_cume_dist"),
        F.dense_rank().over(w_source_listener_signup_date_2).alias("track_bpm_dense_rank"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.sum("max_track_bpm_so_far").over(w_track_release_date_year_devi).alias("max_track_bpm_so_far_sum_4"),
        F.lag("track_release_date_cume_dist").over(w_track_release_date_year_devi_2).alias("prev_track_release_date_cume_dist"),
        F.first("track_bpm_dense_rank").over(w_track_release_date_year_devi_3).alias("first_track_bpm_dense_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        "listener_signup_date",
        "track_release_date",
        "track_bpm",
        "distinct_source_in_device_listener_id",
        "track_release_date_cume_dist",
        "max_track_bpm_so_far",
        "track_bpm_avg_3",
        "distinct_listener_plan_in_device_listener_id",
        "track_bpm_vs_device_listener_id_avg",
        "listener_signup_date_cume_dist",
        "track_bpm_dense_rank",
        "max_track_bpm_so_far_sum_4",
        F.datediff(F.col("listener_signup_date"), F.col("track_release_date")).alias("days_track_release_date_to_listener_signup_date"),
    )
)
