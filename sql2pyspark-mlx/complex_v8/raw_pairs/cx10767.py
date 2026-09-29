from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_all_by_release_date = Window.orderBy("release_date", "id")
w_all_by_release_date_2 = Window.orderBy(F.col("release_date").desc(), "id")
w_all_by_bpm_full = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = df_tracks.filter(F.col("bpm") <= 156)

with_windows = (
    cleaned
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.sum("duration_sec").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_sum_duration_sec"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_all_by_release_date)).alias("duration_sec_change"),
        F.lag("duration_sec", 1, 0).over(w_all_by_release_date_2).alias("prev_duration_sec"),
        F.round(F.avg("bpm").over(w_artist_id_is_explicit), 2).alias("artist_id_is_explicit_avg_bpm"),
        F.dense_rank().over(w_all_by_release_date).alias("release_date_dense_rank"),
        F.min("duration_sec").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_min_duration_sec"),
        F.first("duration_sec").over(w_all_by_bpm_full).alias("first_duration_sec"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_is_explicit), 2).alias("duration_sec_vs_artist_id_is_explicit_avg"),
        (F.col("bpm") - F.lag("bpm").over(w_all_by_release_date_2)).alias("bpm_change"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
    )
)

top_per_group = (
    with_windows
    .filter((F.col("release_date_dense_rank") == 1) & (F.col("artist_id") <= 14))
    .drop("release_date_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "artist_id_is_explicit_sum_duration_sec",
        "duration_sec_change",
        "prev_duration_sec",
        "artist_id_is_explicit_avg_bpm",
        "duration_sec_vs_artist_id_is_explicit_avg",
        "bpm_change",
        "rows_in_artist_id_is_explicit",
        F.trunc(F.col("release_date"), "MM").alias("release_date_month_start"),
        F.array_contains(F.col("moods"), "sad").alias("has_sad"),
    )
    .orderBy(F.col("release_date_month_start").desc(), "id")
)
