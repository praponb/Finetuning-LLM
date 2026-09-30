from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_plus_90d = Window.orderBy(F.col("release_date_plus_90d").desc(), "id").rowsBetween(-1, -1)
w_artist_id_by_release_date_fu = Window.partitionBy("artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_year = Window.partitionBy(F.year("release_date"))
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm")
w_all_by_release_date_plus_90d_2 = Window.orderBy(F.col("release_date_plus_90d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_is_explicit_by_bpm_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm", "id")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id").rowsBetween(-3, -1)
w_all_by_last_bpm_moving_1 = Window.orderBy("last_bpm", "id").rowsBetween(-1, Window.currentRow)
w_artist_id_by_bpm_dense_rank = Window.partitionBy("artist_id").orderBy("bpm_dense_rank", "id").rowsBetween(-6, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_last_bpm = Window.orderBy("last_bpm", "id")

scoped_tracks = (
    df_tracks
    .filter(F.col("release_date").between(F.lit("2024-04-29").cast("date"), F.lit("2025-08-07").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_add(F.col("release_date"), 90).alias("release_date_plus_90d"),
    )
)

windowed = (
    scoped_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "release_date_plus_90d",
        F.round(F.avg("duration_sec").over(w_all_by_release_date_plus_90d), 2).alias("duration_sec_avg_1_prior"),
        F.last("bpm").over(w_artist_id_by_release_date_fu).alias("last_bpm"),
        F.sum("bpm").over(w_release_date_year).alias("release_date_year_sum_bpm"),
        F.dense_rank().over(w_artist_id_is_explicit_by_bpm).alias("bpm_dense_rank"),
        F.last("release_date_plus_90d").over(w_all_by_release_date_plus_90d_2).alias("last_release_date_plus_90d"),
        F.min("bpm").over(w_release_date_year).alias("release_date_year_min_bpm"),
        F.lag("duration_sec").over(w_artist_id_is_explicit_by_bpm_2).alias("prev_duration_sec"),
        F.sum("duration_sec").over(w_is_explicit_by_release_date).alias("duration_sec_sum_3_prior"),
    )
)

derived = (
    windowed
    .filter(F.col("release_date_year_min_bpm") > 669)
    .select(
        "*",
        F.greatest(F.col("duration_sec_sum_3_prior"), F.col("last_bpm")).alias("greatest_duration_sec_sum_3_prior_last_bpm"),
        (F.col("bpm_dense_rank") - F.col("last_bpm")).alias("bpm_dense_rank_minus_last_bpm"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

windowed_2 = (
    derived
    .select(
        "*",
        F.sum("last_bpm").over(w_all_by_last_bpm_moving_1).alias("last_bpm_sum_2"),
        F.round(F.avg("release_date_year_min_bpm").over(w_artist_id_by_bpm_dense_rank), 2).alias("release_date_year_min_bpm_avg_7"),
        F.round(F.col("duration_sec_sum_3_prior") - F.avg("duration_sec_sum_3_prior").over(w_artist_id), 2).alias("duration_sec_sum_3_prior_vs_artist_id_avg"),
        F.lag("release_date", 3).over(w_all_by_last_bpm).alias("prev3_release_date"),
        F.row_number().over(w_all_by_last_bpm).alias("seq_no"),
        F.max("duration_sec_sum_3_prior").over(w_artist_id).alias("artist_id_max_duration_sec_sum_3_prior"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "release_date",
        "is_explicit",
        "moods",
        "duration_sec_avg_1_prior",
        "release_date_year_sum_bpm",
        "bpm_dense_rank",
        "last_release_date_plus_90d",
        "release_date_year_min_bpm",
        "prev_duration_sec",
        "bpm_dense_rank_minus_last_bpm",
        "days_since_release_date",
        "last_bpm_sum_2",
        "release_date_year_min_bpm_avg_7",
        "duration_sec_sum_3_prior_vs_artist_id_avg",
        "prev3_release_date",
        "artist_id_max_duration_sec_sum_3_prior",
        F.quarter(F.col("release_date_plus_90d")).alias("release_date_plus_90d_quarter"),
    )
    .orderBy(
        F.col("release_date_year_min_bpm").asc(),
        F.col("bpm_dense_rank_minus_last_bpm").asc_nulls_last(),
    )
)
