from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_track_release_da = Window.partitionBy("track_id").orderBy(F.col("track_release_date").desc(), "id")
w_track_artist_id_listener_id = Window.partitionBy("track_artist_id", "listener_id").orderBy(F.col("ms_played_bucket").asc_nulls_last(), "id")
w_device = Window.partitionBy("device")
w_track_id_by_ms_played_bucket = Window.partitionBy("track_id").orderBy(F.col("ms_played_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id = Window.partitionBy("listener_id")
w_track_id_by_rows_in_device_p = Window.partitionBy("track_id").orderBy(F.col("rows_in_device").desc(), "id").rowsBetween(-3, -1)
w_track_release_date_year_by_m = Window.partitionBy(F.year("track_release_date")).orderBy("ms_played_bucket_seq", "id")
w_track_id_by_rows_in_device_p_2 = Window.partitionBy("track_id").orderBy(F.col("rows_in_device").desc(), "id").rowsBetween(-6, -1)
w_track_id_by_rows_in_device_f = Window.partitionBy("track_id").orderBy(F.col("rows_in_device").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_plays
    .filter(F.col("ms_played").between(62294, 360286))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 145720).alias("ms_played_distance"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

with_details_track = (
    base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.ms_played_distance",
        "s.ms_played_bucket",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

with_windows = (
    with_details_track
    .select(
        "*",
        (F.col("ms_played_bucket") - F.lag("ms_played_bucket").over(w_track_id_by_track_release_da)).alias("ms_played_bucket_change"),
        F.dense_rank().over(w_track_artist_id_listener_id).alias("ms_played_bucket_dense_rank"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.max("ms_played_bucket").over(w_device).alias("device_max_ms_played_bucket"),
        F.last("track_release_date", ignorenulls=True).over(w_track_id_by_ms_played_bucket).alias("last_known_track_release_date"),
        F.row_number().over(w_track_artist_id_listener_id).alias("ms_played_bucket_seq"),
    )
)

first_rows = (
    with_windows
    .filter((F.col("ms_played_bucket_seq") <= 2) & (F.col("track_release_date").between(F.lit("2024-05-12").cast("date"), F.lit("2025-03-29").cast("date"))))
)

sequenced = (
    first_rows
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "ms_played_distance",
        "ms_played_bucket",
        "track_moods",
        "track_artist_id",
        "ms_played_bucket_change",
        "ms_played_bucket_dense_rank",
        "last_known_track_release_date",
        "ms_played_bucket_seq",
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.sum("ms_played_bucket_change").over(w_listener_id).alias("listener_id_sum_ms_played_bucket_change"),
        F.round(F.avg("ms_played_distance").over(w_track_id_by_rows_in_device_p), 2).alias("ms_played_distance_avg_3_prior"),
        F.row_number().over(w_track_release_date_year_by_m).alias("rn"),
        F.max("rows_in_device").over(w_track_id_by_rows_in_device_p_2).alias("rows_in_device_max_6_prior"),
        F.nth_value("ms_played_bucket_change", 2).over(w_track_id_by_rows_in_device_f).alias("second_ms_played_bucket_change"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "device",
        "ms_played_distance",
        "ms_played_bucket",
        "track_moods",
        "track_artist_id",
        "ms_played_bucket_change",
        "last_known_track_release_date",
        "ms_played_bucket_seq",
        "rows_in_listener_id",
        "listener_id_sum_ms_played_bucket_change",
        "rn",
        "rows_in_device_max_6_prior",
        "second_ms_played_bucket_change",
        F.when(F.col("ms_played_bucket") >= 281500, "large").when(F.col("ms_played_bucket") >= 143250, "medium").otherwise("small").alias("ms_played_bucket_tier"),
        F.when(F.col("device").isin("desktop", "car"), "group_a").when(F.col("device") == "phone", "group_b").otherwise("other").alias("device_group"),
    )
    .orderBy(F.col("rn").desc(), "ms_played_bucket_seq")
)
