from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_artist_id_by_dur_2 = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec")
w_duration_sec_seq = Window.partitionBy("duration_sec_seq")
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_all_by_duration_sec_prior_3 = Window.orderBy("duration_sec", "id").rowsBetween(-3, -1)
w_all_by_duration_sec_2 = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id")

recent = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 1) & (F.col("duration_sec") >= 213))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

with_windows = (
    recent
    .select(
        "id",
        "duration_sec",
        "bpm",
        "moods",
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.round(F.percent_rank().over(w_artist_id_by_duration_sec), 3).alias("duration_sec_percent_rank"),
        F.first("duration_sec", ignorenulls=True).over(w_is_explicit_artist_id_by_dur).alias("first_known_duration_sec"),
        F.sum("duration_sec").over(w_is_explicit_artist_id_by_dur).alias("running_duration_sec"),
        F.round(F.avg("bpm").over(w_is_explicit), 2).alias("is_explicit_avg_bpm"),
        F.lag("bpm").over(w_artist_id_by_duration_sec).alias("prev_bpm"),
        F.nth_value("duration_sec", 2).over(w_artist_id_is_explicit_by_dur).alias("second_duration_sec"),
        F.row_number().over(w_artist_id_by_duration_sec).alias("duration_sec_seq"),
        F.round(F.cume_dist().over(w_is_explicit_artist_id_by_dur_2), 3).alias("duration_sec_cume_dist"),
    )
)

first_rows = with_windows.filter(F.col("duration_sec_seq") <= 5)

with_windows_2 = (
    first_rows
    .select(
        "*",
        F.round(F.col("running_duration_sec") / F.nullif(F.sum("running_duration_sec").over(w_duration_sec_seq), F.lit(0)), 4).alias("running_duration_sec_share_of_duration_sec_seq"),
        F.rank().over(w_all_by_duration_sec).alias("duration_sec_rank"),
        F.count(F.lit(1)).over(w_duration_sec_seq).alias("rows_in_duration_sec_seq"),
        F.row_number().over(w_all_by_duration_sec).alias("rn"),
        F.round(F.avg("duration_sec").over(w_all_by_duration_sec_prior_3), 2).alias("duration_sec_avg_3_prior"),
        F.lag("duration_sec_seq").over(w_all_by_duration_sec).alias("prev_duration_sec_seq"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_2), 3).alias("duration_sec_percent_rank_2"),
    )
)

summary = (
    with_windows_2
    .groupBy("duration_sec_rank", "duration_sec_seq", "rn")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("duration_sec_avg_3_prior"), 2).alias("avg_duration_sec_avg_3_prior"),
        F.round(F.stddev("prev_duration_sec_seq"), 2).alias("stddev_prev_duration_sec_seq"),
    )
)

result = (
    summary
    .select(
        "duration_sec_rank",
        "duration_sec_seq",
        "rn",
        "cnt",
        "stddev_prev_duration_sec_seq",
        F.least(F.col("duration_sec_rank"), F.col("cnt")).alias("least_duration_sec_rank_cnt"),
        F.coalesce(F.col("duration_sec_seq"), F.lit(0)).alias("duration_sec_seq_filled"),
    )
    .orderBy(F.col("cnt").desc())
)
