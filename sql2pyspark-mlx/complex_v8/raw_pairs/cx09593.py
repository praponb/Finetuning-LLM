from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_bpm_running = Window.partitionBy("is_explicit").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_prior_3 = Window.orderBy("duration_sec", "id").rowsBetween(-3, -1)
w_all_by_bpm_prior_6 = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(-6, -1)
w_is_explicit = Window.partitionBy("is_explicit")
w_all_by_duration_sec_full = Window.orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_bpm_full = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_ties = Window.orderBy(F.col("bpm").desc())
w_bpm_dense_rank_is_explicit_b = Window.partitionBy("bpm_dense_rank", "is_explicit").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_bpm_dense_rank = Window.partitionBy("bpm_dense_rank")
w_duration_sec_dense_rank_by_l = Window.partitionBy("duration_sec_dense_rank").orderBy("last_known_duration_sec", "id").rowsBetween(-2, -1)
w_is_explicit_duration_sec_den = Window.partitionBy("is_explicit", "duration_sec_dense_rank").orderBy(F.col("artist_id_sum_duration_sec").desc())
w_is_explicit_duration_sec_den_2 = Window.partitionBy("is_explicit", "duration_sec_dense_rank").orderBy(F.col("artist_id_sum_duration_sec").desc(), "id")

base = (
    df_tracks
    .filter(F.col("duration_sec") < 409)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
    )
)

with_windows = (
    base
    .select(
        "*",
        F.last("duration_sec", ignorenulls=True).over(w_is_explicit_by_bpm_running).alias("last_known_duration_sec"),
        F.max("bpm").over(w_all_by_duration_sec_prior_3).alias("bpm_max_3_prior"),
        F.round(F.avg("duration_sec").over(w_all_by_bpm_prior_6), 2).alias("duration_sec_avg_6_prior"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.nth_value("duration_sec", 2).over(w_all_by_duration_sec_full).alias("second_duration_sec"),
        F.dense_rank().over(w_all_by_duration_sec_ties).alias("duration_sec_dense_rank"),
        F.sum("duration_sec").over(w_artist_id).alias("artist_id_sum_duration_sec"),
        F.nth_value("bpm", 2).over(w_all_by_bpm_full).alias("second_bpm"),
        F.dense_rank().over(w_all_by_bpm_ties).alias("bpm_dense_rank"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.max("second_duration_sec").over(w_bpm_dense_rank_is_explicit_b).alias("max_second_duration_sec_so_far"),
        F.round(F.col("last_known_duration_sec") / F.nullif(F.sum("last_known_duration_sec").over(w_bpm_dense_rank), F.lit(0)), 4).alias("last_known_duration_sec_share_of_bpm_dense_rank"),
        F.round(F.col("artist_id_sum_duration_sec") - F.avg("artist_id_sum_duration_sec").over(w_artist_id), 2).alias("artist_id_sum_duration_sec_vs_artist_id_avg"),
        F.sum("rows_in_is_explicit").over(w_duration_sec_dense_rank_by_l).alias("rows_in_is_explicit_sum_2_prior"),
        F.round(F.percent_rank().over(w_is_explicit_duration_sec_den), 3).alias("artist_id_sum_duration_sec_percent_rank"),
        F.min("last_known_duration_sec").over(w_bpm_dense_rank).alias("bpm_dense_rank_min_last_known_duration_sec"),
        (F.col("second_duration_sec") - F.lag("second_duration_sec").over(w_is_explicit_duration_sec_den_2)).alias("second_duration_sec_change"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "bpm_max_3_prior",
        "duration_sec_avg_6_prior",
        "second_duration_sec",
        "duration_sec_dense_rank",
        "artist_id_sum_duration_sec",
        "max_second_duration_sec_so_far",
        "last_known_duration_sec_share_of_bpm_dense_rank",
        "artist_id_sum_duration_sec_vs_artist_id_avg",
        "rows_in_is_explicit_sum_2_prior",
        "second_duration_sec_change",
        F.greatest(F.col("duration_sec_avg_6_prior"), F.col("bpm")).alias("greatest_duration_sec_avg_6_prior_bpm"),
    )
)
