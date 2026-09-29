from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_ms_played = Window.orderBy(F.col("total_ms_played").desc(), "track_id")
w_all_by_total_ms_played_full = Window.orderBy(F.col("total_ms_played").desc(), "track_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id = Window.partitionBy("track_id")
w_track_artist_id = Window.partitionBy("track_artist_id")
w_all_by_total_ms_played_vs_tr = Window.orderBy(F.col("total_ms_played_vs_track_id_avg").desc_nulls_first(), "track_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_total_ms_played_vs_tr_2 = Window.orderBy(F.col("total_ms_played_vs_track_id_avg").desc_nulls_first(), "track_id")

recent = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

summary = (
    recent
    .filter(F.col("is_skipped"))
    .groupBy("track_id")
    .agg(
        F.round(F.stddev("ms_played"), 2).alias("stddev_ms_played"),
        F.sum("ms_played").alias("total_ms_played"),
    )
)

with_windows = (
    summary
    .select(
        "track_id",
        "stddev_ms_played",
        "total_ms_played",
        (F.col("total_ms_played") - F.lag("total_ms_played").over(w_all_by_total_ms_played)).alias("total_ms_played_change"),
        F.last("total_ms_played").over(w_all_by_total_ms_played_full).alias("last_total_ms_played"),
        F.round(F.col("total_ms_played") - F.avg("total_ms_played").over(w_track_id), 2).alias("total_ms_played_vs_track_id_avg"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
    )
)

with_parent_track = (
    with_windows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.track_id",
        "s.stddev_ms_played",
        "s.total_ms_played",
        "s.total_ms_played_change",
        "s.last_total_ms_played",
        "s.total_ms_played_vs_track_id_avg",
        "s.rows_in_track_id",
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

ranked = (
    with_parent_track
    .select(
        "track_id",
        "stddev_ms_played",
        "total_ms_played",
        "total_ms_played_change",
        "last_total_ms_played",
        "total_ms_played_vs_track_id_avg",
        "rows_in_track_id",
        "track_artist_id",
        F.count(F.lit(1)).over(w_track_artist_id).alias("rows_in_track_artist_id"),
        F.nth_value("total_ms_played", 2).over(w_all_by_total_ms_played_vs_tr).alias("second_total_ms_played"),
        F.max("last_total_ms_played").over(w_track_artist_id).alias("track_artist_id_max_last_total_ms_played"),
        F.sum("rows_in_track_id").over(w_track_artist_id).alias("track_artist_id_sum_rows_in_track_id"),
        F.row_number().over(w_all_by_total_ms_played_vs_tr_2).alias("seq_no"),
    )
)

labeled = (
    ranked
    .filter(F.col("track_artist_id") >= 9)
    .select(
        "track_id",
        "total_ms_played_change",
        "last_total_ms_played",
        "rows_in_track_id",
        "rows_in_track_artist_id",
        "second_total_ms_played",
        "track_artist_id_max_last_total_ms_played",
        "track_artist_id_sum_rows_in_track_id",
        "seq_no",
        F.greatest(F.col("second_total_ms_played"), F.col("rows_in_track_id")).alias("greatest_second_total_ms_played_rows_in_track_id"),
        F.abs(F.col("rows_in_track_id") - 7).alias("rows_in_track_id_distance"),
    )
)

result = (
    labeled
    .select(
        "track_id",
        "total_ms_played_change",
        "last_total_ms_played",
        "rows_in_track_id",
        "second_total_ms_played",
        "track_artist_id_sum_rows_in_track_id",
        "seq_no",
        "greatest_second_total_ms_played_rows_in_track_id",
        "rows_in_track_id_distance",
    )
    .orderBy(F.col("rows_in_track_id_distance").asc_nulls_last(), "track_id")
)
