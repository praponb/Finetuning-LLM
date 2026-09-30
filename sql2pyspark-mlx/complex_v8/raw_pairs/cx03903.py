from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm")
w_all_by_release_date_moving_2 = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_release_date = Window.orderBy("release_date", "id")
w_artist_id_is_explicit_by_bpm_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date_ties = Window.orderBy(F.col("release_date").desc())

base = df_tracks.filter(F.col("is_explicit"))

windowed = (
    base
    .select(
        "*",
        F.rank().over(w_is_explicit_artist_id_by_dur).alias("duration_sec_rank"),
        F.round(F.avg("duration_sec").over(w_is_explicit_artist_id), 2).alias("is_explicit_artist_id_avg_duration_sec"),
        F.round(F.avg("bpm").over(w_is_explicit_artist_id), 2).alias("is_explicit_artist_id_avg_bpm"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
        F.round(F.cume_dist().over(w_artist_id_is_explicit_by_bpm), 3).alias("bpm_cume_dist"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit_artist_id), F.lit(0)), 4).alias("bpm_share_of_is_explicit_artist_id"),
        F.sum("duration_sec").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_sum_duration_sec"),
        F.sum("duration_sec").over(w_all_by_release_date_moving_2).alias("duration_sec_sum_3"),
        F.lag("bpm").over(w_all_by_release_date).alias("prev_bpm"),
        F.nth_value("bpm", 2).over(w_artist_id_is_explicit_by_bpm_2).alias("second_bpm"),
        F.rank().over(w_all_by_release_date_ties).alias("release_date_rank"),
    )
)

first_rows = (
    windowed
    .filter(F.col("duration_sec_rank") == 1)
    .drop("duration_sec_rank")
)

result = (
    first_rows
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "is_explicit_artist_id_avg_duration_sec",
        "rows_in_is_explicit_artist_id",
        "bpm_cume_dist",
        "bpm_share_of_is_explicit_artist_id",
        "is_explicit_artist_id_sum_duration_sec",
        "second_bpm",
        "release_date_rank",
    )
    .orderBy(F.col("second_bpm").asc(), "bpm_cume_dist")
)
