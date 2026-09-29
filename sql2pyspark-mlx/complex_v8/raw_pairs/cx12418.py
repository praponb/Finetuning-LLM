from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_artist_genre_by_track = Window.partitionBy("device", "artist_genre").orderBy(F.col("track_duration_sec").desc(), "id")
w_device_artist_genre_by_track_2 = Window.partitionBy("device", "artist_genre").orderBy(F.col("track_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")
w_track_artist_id_by_track_dur = Window.partitionBy("track_artist_id").orderBy(F.col("track_duration_sec").desc(), "id")
w_track_artist_id_device_by_pl = Window.partitionBy("track_artist_id", "device").orderBy(F.col("played_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_track_artist_id_device_by_pl_2 = Window.partitionBy("track_artist_id", "device").orderBy(F.col("played_at").desc())
w_track_id_device = Window.partitionBy("track_id", "device")
w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_artist_genre_by_track_3 = Window.partitionBy("device", "artist_genre").orderBy(F.col("track_duration_sec").desc())
w_track_artist_id_device_by_pl_3 = Window.partitionBy("track_artist_id", "device").orderBy(F.col("played_at").desc(), "id")
w_all_by_seq_no_running = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_dense_rank_by_seq = Window.partitionBy("played_at_dense_rank").orderBy("seq_no", "id").rowsBetween(-2, -1)
w_all_by_seq_no_ties = Window.orderBy("seq_no")
w_played_at_dense_rank_by_seq_2 = Window.partitionBy("played_at_dense_rank").orderBy("seq_no", "id")
w_track_artist_id_by_played_at = Window.partitionBy("track_artist_id").orderBy(F.col("played_at_dense_rank").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_track_artist_id_by_played_at_2 = Window.partitionBy("track_artist_id").orderBy(F.col("played_at_dense_rank").asc_nulls_last())
w_seq_no = Window.partitionBy("seq_no")

prepared = (
    df_plays
    .filter(F.col("device") == "car")
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

combined_details = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
    )
)

windowed = (
    combined_details
    .select(
        "id",
        "played_at",
        "track_duration_sec",
        "track_moods",
        "track_artist_id",
        F.row_number().over(w_device_artist_genre_by_track).alias("seq_no"),
        F.sum("track_duration_sec").over(w_device_artist_genre_by_track_2).alias("running_track_duration_sec"),
        F.round(F.col("track_duration_sec") - F.avg("track_duration_sec").over(w_track_id_listener_id), 2).alias("track_duration_sec_vs_track_id_listener_id_avg"),
        F.ntile(4).over(w_track_artist_id_by_track_dur).alias("track_duration_sec_quartile"),
        F.round(F.avg("track_duration_sec").over(w_track_artist_id_device_by_pl), 2).alias("track_duration_sec_avg_3"),
        F.dense_rank().over(w_track_artist_id_device_by_pl_2).alias("played_at_dense_rank"),
        F.sum("track_duration_sec").over(w_track_id_listener_id).alias("track_id_listener_id_sum_track_duration_sec"),
        F.count(F.lit(1)).over(w_track_id_device).alias("rows_in_track_id_device"),
        F.first("track_duration_sec", ignorenulls=True).over(w_all_by_played_at_running).alias("first_known_track_duration_sec"),
        F.round(F.percent_rank().over(w_device_artist_genre_by_track_3), 3).alias("track_duration_sec_percent_rank"),
        F.ntile(4).over(w_track_artist_id_device_by_pl_3).alias("played_at_quartile"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.first("played_at_dense_rank", ignorenulls=True).over(w_all_by_seq_no_running).alias("first_known_played_at_dense_rank"),
        F.round(F.avg("track_duration_sec_quartile").over(w_played_at_dense_rank_by_seq), 2).alias("track_duration_sec_quartile_avg_2_prior"),
        F.rank().over(w_all_by_seq_no_ties).alias("seq_no_rank"),
        F.lead("running_track_duration_sec").over(w_played_at_dense_rank_by_seq_2).alias("next_running_track_duration_sec"),
        F.max("running_track_duration_sec").over(w_track_artist_id_by_played_at).alias("running_track_duration_sec_max_7"),
        F.lag("track_duration_sec", 2, 0).over(w_played_at_dense_rank_by_seq_2).alias("prev2_track_duration_sec"),
        F.round(F.cume_dist().over(w_track_artist_id_by_played_at_2), 3).alias("played_at_dense_rank_cume_dist"),
        F.min("played_at_quartile").over(w_seq_no).alias("seq_no_min_played_at_quartile"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "played_at",
        "track_duration_sec",
        "track_moods",
        "seq_no",
        "running_track_duration_sec",
        "track_duration_sec_quartile",
        "played_at_dense_rank",
        "rows_in_track_id_device",
        "first_known_track_duration_sec",
        "track_duration_sec_percent_rank",
        "played_at_quartile",
        "first_known_played_at_dense_rank",
        "track_duration_sec_quartile_avg_2_prior",
        "seq_no_rank",
        "running_track_duration_sec_max_7",
        "prev2_track_duration_sec",
        "played_at_dense_rank_cume_dist",
        F.date_trunc("day", F.col("played_at")).alias("played_at_day"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
    .orderBy(F.col("seq_no_rank").desc(), "id")
)
