from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("release_date").desc())
w_all_by_release_date_full = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date_ties = Window.orderBy("release_date")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc())
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy("bpm", "id")
w_all_by_release_date = Window.orderBy("release_date", "id")
w_is_explicit = Window.partitionBy("is_explicit")
w_all_by_release_date_prior_2 = Window.orderBy("release_date", "id").rowsBetween(-2, -1)
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_release_date_month_artist_id = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_id")
w_artist_id_is_explicit_by_dur_2 = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec").asc_nulls_last())
w_all_by_second_bpm = Window.orderBy(F.col("second_bpm").asc_nulls_last(), "id")
w_is_explicit_release_date_ran = Window.partitionBy("is_explicit", "release_date_rank")

base_tracks = (
    df_tracks
    .filter(F.col("duration_sec") > 184)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

metrics = (
    base_tracks
    .select(
        "*",
        F.round(F.cume_dist().over(w_artist_id_is_explicit_by_rel), 3).alias("release_date_cume_dist"),
        F.nth_value("bpm", 2).over(w_all_by_release_date_full).alias("second_bpm"),
        F.round(F.percent_rank().over(w_all_by_release_date_ties), 3).alias("release_date_percent_rank"),
        F.rank().over(w_is_explicit_by_release_date).alias("release_date_rank"),
        F.row_number().over(w_is_explicit_by_bpm).alias("seq_no"),
        F.dense_rank().over(w_all_by_release_date).alias("release_date_dense_rank"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_is_explicit), 2).alias("bpm_vs_is_explicit_avg"),
        F.round(F.avg("duration_sec").over(w_all_by_release_date_prior_2), 2).alias("duration_sec_avg_2_prior"),
        F.lag("duration_sec", 1, 0).over(w_is_explicit_by_release_date_2).alias("prev_duration_sec"),
        F.nth_value("release_date", 2).over(w_all_by_release_date_full).alias("second_release_date"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.dense_rank().over(w_is_explicit_by_release_date_2).alias("release_date_dense_rank_2"),
        F.nth_value("duration_sec", 2).over(w_all_by_release_date_full).alias("second_duration_sec"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_is_explicit_by_dur)).alias("bpm_change"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_release_date_month_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_release_date_month_artist_"),
        F.dense_rank().over(w_artist_id_is_explicit_by_dur_2).alias("duration_sec_dense_rank"),
    )
)

top_rows = (
    metrics
    .filter(F.col("duration_sec_dense_rank") <= 2)
    .drop("duration_sec_dense_rank")
)

metrics_2 = (
    top_rows
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "moods",
        "release_date_label",
        "release_date_cume_dist",
        "release_date_percent_rank",
        "release_date_rank",
        "release_date_dense_rank",
        "duration_sec_avg_2_prior",
        "prev_duration_sec",
        "second_release_date",
        "rows_in_artist_id",
        "release_date_dense_rank_2",
        "second_duration_sec",
        "bpm_change",
        "duration_sec_share_of_release_date_month_artist_",
        F.round(F.cume_dist().over(w_all_by_second_bpm), 3).alias("second_bpm_cume_dist"),
        F.count(F.lit(1)).over(w_is_explicit_release_date_ran).alias("rows_in_is_explicit_release_date_rank"),
        F.min("prev_duration_sec").over(w_is_explicit_release_date_ran).alias("is_explicit_release_date_rank_min_prev_duration_"),
    )
)

result = (
    metrics_2
    .filter(F.col("prev_duration_sec") < 353)
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "moods",
        "release_date_cume_dist",
        "release_date_percent_rank",
        "release_date_rank",
        "prev_duration_sec",
        "second_release_date",
        "rows_in_artist_id",
        "release_date_dense_rank_2",
        "bpm_change",
        "duration_sec_share_of_release_date_month_artist_",
        "second_bpm_cume_dist",
        "rows_in_is_explicit_release_date_rank",
        "is_explicit_release_date_rank_min_prev_duration_",
    )
    .orderBy(F.col("release_date_dense_rank_2").asc_nulls_last())
)
