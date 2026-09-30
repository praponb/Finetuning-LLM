from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date", "id")
w_artist_id_by_bpm_running = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit = Window.partitionBy("is_explicit")
w_is_explicit_artist_id_by_bpm = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").desc(), "id")
w_is_explicit_artist_id_by_rel_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id")
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy("release_date")
w_is_explicit_by_release_date_3 = Window.partitionBy("is_explicit").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

with_windows = (
    cleaned_tracks
    .select(
        "*",
        F.nth_value("bpm", 2).over(w_is_explicit_artist_id_by_rel).alias("second_bpm"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.round(F.cume_dist().over(w_is_explicit_by_release_date), 3).alias("release_date_cume_dist"),
        F.min("bpm").over(w_artist_id_by_bpm_running).alias("min_bpm_so_far"),
        F.round(F.avg("bpm").over(w_is_explicit), 2).alias("is_explicit_avg_bpm"),
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.lag("release_date").over(w_is_explicit_artist_id_by_bpm).alias("prev_release_date"),
        F.lag("bpm", 1, 0).over(w_is_explicit_artist_id_by_rel_2).alias("prev_bpm"),
        F.dense_rank().over(w_is_explicit_by_release_date_2).alias("release_date_dense_rank"),
        F.rank().over(w_is_explicit_by_release_date_2).alias("release_date_rank"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_is_explicit), 2).alias("bpm_vs_is_explicit_avg"),
        F.row_number().over(w_is_explicit_by_release_date).alias("rn"),
        F.last("release_date").over(w_is_explicit_by_release_date_3).alias("last_release_date"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
    )
)

first_rows = with_windows.filter(F.col("release_date_rank") <= 5)

result = (
    first_rows
    .select(
        "id",
        "release_date",
        "is_explicit",
        "bpm",
        "release_date_cume_dist",
        "is_explicit_avg_bpm",
        "distinct_artist_id_in_is_explicit",
        "prev_bpm",
        "release_date_dense_rank",
        "release_date_rank",
        "last_release_date",
        "rows_in_is_explicit",
    )
    .orderBy(F.col("release_date_rank").desc(), F.col("last_release_date").asc())
)
