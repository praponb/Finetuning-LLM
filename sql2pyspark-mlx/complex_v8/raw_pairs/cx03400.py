from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_is_explicit_device_by = Window.partitionBy("track_is_explicit", "device").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_listener_id_by_played = Window.partitionBy("device", "listener_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(-3, -1)
w_track_is_explicit_by_track_b = Window.partitionBy("track_is_explicit").orderBy(F.col("track_bpm").desc(), "id")
w_listener_id_track_id = Window.partitionBy("listener_id", "track_id")
w_all_by_played_at_running = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_track_bpm = Window.orderBy(F.col("track_bpm").desc(), "id")
w_track_is_explicit = Window.partitionBy("track_is_explicit")
w_listener_id = Window.partitionBy("listener_id")
w_track_artist_id_listener_id = Window.partitionBy("track_artist_id", "listener_id").orderBy("prev_track_bpm_plus_prev_track_bpm_2", "id")
w_all_by_hours_second_played_a = Window.orderBy(F.col("hours_second_played_at_to_played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_hours_second_played_a_2 = Window.orderBy(F.col("hours_second_played_at_to_played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

combined_details_track = (
    recent.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

ranked = (
    combined_details_track
    .select(
        "*",
        F.nth_value("played_at", 2).over(w_track_is_explicit_device_by).alias("second_played_at"),
        F.sum("track_bpm").over(w_device_listener_id_by_played).alias("track_bpm_sum_3_prior"),
        F.ntile(3).over(w_track_is_explicit_by_track_b).alias("track_bpm_tercile"),
        F.round(F.col("track_bpm") - F.avg("track_bpm").over(w_listener_id_track_id), 2).alias("track_bpm_vs_listener_id_track_id_avg"),
        F.min("track_bpm").over(w_all_by_played_at_running).alias("min_track_bpm_so_far"),
        F.lag("track_bpm", 1, 0).over(w_all_by_track_bpm).alias("prev_track_bpm"),
        F.round(F.col("track_bpm") - F.avg("track_bpm").over(w_track_is_explicit), 2).alias("track_bpm_vs_track_is_explicit_avg"),
        F.lag("track_bpm", 1, 0).over(w_track_is_explicit_by_track_b).alias("prev_track_bpm_2"),
    )
)

scored = (
    ranked
    .filter(F.col("track_bpm_sum_3_prior").between(180, 468))
    .select(
        "id",
        "listener_id",
        "track_id",
        "track_artist_id",
        "track_bpm",
        "second_played_at",
        "track_bpm_vs_listener_id_track_id_avg",
        "min_track_bpm_so_far",
        "track_bpm_vs_track_is_explicit_avg",
        (F.col("prev_track_bpm") + F.col("prev_track_bpm_2")).alias("prev_track_bpm_plus_prev_track_bpm_2"),
        F.round((F.unix_timestamp(F.col("played_at")) - F.unix_timestamp(F.col("second_played_at"))) / 3600, 1).alias("hours_second_played_at_to_played_at"),
    )
)

windowed = (
    scored
    .select(
        "id",
        "listener_id",
        "track_id",
        "second_played_at",
        "min_track_bpm_so_far",
        "track_bpm_vs_track_is_explicit_avg",
        "prev_track_bpm_plus_prev_track_bpm_2",
        "hours_second_played_at_to_played_at",
        F.sum("min_track_bpm_so_far").over(w_listener_id).alias("listener_id_sum_min_track_bpm_so_far"),
        F.row_number().over(w_track_artist_id_listener_id).alias("seq_no"),
        F.last("track_bpm", ignorenulls=True).over(w_all_by_hours_second_played_a).alias("last_known_track_bpm"),
        F.nth_value("track_bpm", 2).over(w_all_by_hours_second_played_a_2).alias("second_track_bpm"),
    )
)

stats = (
    windowed
    .groupBy("seq_no")
    .agg(
        F.min("track_bpm_vs_track_is_explicit_avg").alias("min_track_bpm_vs_track_is_explicit_avg"),
        F.max("min_track_bpm_so_far").alias("max_min_track_bpm_so_far"),
        F.round(F.stddev("min_track_bpm_so_far"), 2).alias("stddev_min_track_bpm_so_far"),
        F.percentile_approx("listener_id_sum_min_track_bpm_so_far", 0.5).alias("median_listener_id_sum_min_track_bpm_so_far"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 3)
)

result = (
    stats
    .select(
        "seq_no",
        "min_track_bpm_vs_track_is_explicit_avg",
        "stddev_min_track_bpm_so_far",
        "n_rows",
        (F.col("seq_no") - F.col("n_rows")).alias("seq_no_minus_n_rows"),
        (F.floor(F.col("seq_no") / 2) * 2).alias("seq_no_bucket"),
    )
    .orderBy(F.col("n_rows").asc_nulls_last(), F.col("seq_no_bucket").desc())
)
