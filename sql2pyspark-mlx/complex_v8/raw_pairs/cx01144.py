from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_listener_id_by_track = Window.partitionBy("source", "listener_id").orderBy("track_bpm")
w_track_artist_id = Window.partitionBy("track_artist_id")
w_track_artist_id_by_track_bpm = Window.partitionBy("track_artist_id").orderBy(F.col("track_bpm").asc_nulls_last(), "id")
w_track_id_track_artist_id_by = Window.partitionBy("track_id", "track_artist_id").orderBy("track_bpm")
w_source_listener_id_by_track_2 = Window.partitionBy("source", "listener_id").orderBy("track_bpm", "id")
w_device_source_by_track_bpm = Window.partitionBy("device", "source").orderBy(F.col("track_bpm").desc(), "id")
w_track_id_listener_id_by_trac = Window.partitionBy("track_id", "listener_id").orderBy(F.col("track_artist_id_max_track_bpm").desc(), "id")
w_all_by_track_bpm_dense_rank = Window.orderBy(F.col("track_bpm_dense_rank_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_bpm_rank_track_bpm_den = Window.partitionBy("track_bpm_rank", "track_bpm_dense_rank_2")
w_all_by_track_bpm_dense_rank_2 = Window.orderBy("track_bpm_dense_rank_2", "id").rowsBetween(-2, Window.currentRow)
w_track_id_listener_id_by_trac_2 = Window.partitionBy("track_id", "listener_id").orderBy(F.col("track_artist_id_max_track_bpm").desc())

prepared = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
    )
)

combined_details_track = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.release_date") >= F.lit("2024-06-03").cast("date")), "inner")
    .select(
        "s.*",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

with_windows = (
    combined_details_track
    .select(
        "*",
        F.dense_rank().over(w_source_listener_id_by_track).alias("track_bpm_dense_rank"),
        F.max("track_bpm").over(w_track_artist_id).alias("track_artist_id_max_track_bpm"),
        F.lag("track_bpm").over(w_track_artist_id_by_track_bpm).alias("prev_track_bpm"),
        F.count(F.lit(1)).over(w_track_artist_id).alias("rows_in_track_artist_id"),
        F.rank().over(w_track_id_track_artist_id_by).alias("track_bpm_rank"),
        F.round(F.cume_dist().over(w_source_listener_id_by_track_2), 3).alias("track_bpm_cume_dist"),
        F.dense_rank().over(w_track_id_track_artist_id_by).alias("track_bpm_dense_rank_2"),
        F.round(F.percent_rank().over(w_device_source_by_track_bpm), 3).alias("track_bpm_percent_rank"),
    )
)

tagged = (
    with_windows
    .select(
        "*",
        F.coalesce(F.col("track_bpm_percent_rank"), F.lit(0.0)).alias("track_bpm_percent_rank_filled"),
        F.concat(F.col("source"), F.lit("#"), F.col("track_bpm").cast("string")).alias("source_ref"),
    )
)

with_windows_2 = (
    tagged
    .select(
        "id",
        "listener_id",
        "track_id",
        "source",
        "track_artist_id",
        "track_bpm_dense_rank",
        "track_artist_id_max_track_bpm",
        "rows_in_track_artist_id",
        "track_bpm_rank",
        "track_bpm_cume_dist",
        "track_bpm_dense_rank_2",
        "track_bpm_percent_rank",
        "track_bpm_percent_rank_filled",
        F.rank().over(w_track_id_listener_id_by_trac).alias("track_artist_id_max_track_bpm_rank"),
        (F.col("prev_track_bpm") - F.lag("prev_track_bpm").over(w_track_id_listener_id_by_trac)).alias("prev_track_bpm_change"),
        F.last("track_bpm_percent_rank").over(w_all_by_track_bpm_dense_rank).alias("last_track_bpm_percent_rank"),
        F.min("track_bpm_rank").over(w_track_bpm_rank_track_bpm_den).alias("track_bpm_rank_track_bpm_dense_rank_2_min_track_"),
        F.sum("track_bpm_dense_rank_2").over(w_all_by_track_bpm_dense_rank_2).alias("track_bpm_dense_rank_2_sum_3"),
        F.sum("track_bpm_cume_dist").over(w_track_bpm_rank_track_bpm_den).alias("track_bpm_rank_track_bpm_dense_rank_2_sum_track_"),
        F.round(F.cume_dist().over(w_track_id_listener_id_by_trac_2), 3).alias("track_artist_id_max_track_bpm_cume_dist"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "listener_id",
        "track_id",
        "source",
        "track_bpm_dense_rank",
        "track_artist_id_max_track_bpm",
        "track_bpm_cume_dist",
        "track_bpm_dense_rank_2",
        "track_bpm_percent_rank",
        "track_bpm_percent_rank_filled",
        "track_artist_id_max_track_bpm_rank",
        "prev_track_bpm_change",
        "last_track_bpm_percent_rank",
        "track_bpm_dense_rank_2_sum_3",
        "track_bpm_rank_track_bpm_dense_rank_2_sum_track_",
        "track_artist_id_max_track_bpm_cume_dist",
    )
    .orderBy(F.col("track_bpm_cume_dist").desc())
)
