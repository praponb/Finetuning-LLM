from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec_mi = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_minus_bpm").asc_nulls_last())
w_artist_id_by_duration_sec_mi_2 = Window.partitionBy("artist_id").orderBy("duration_sec_minus_bpm", "id")
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date"))
w_all_by_release_date_running = Window.orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_is_explicit_by_bpm_2 = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm").desc(), "id")
w_all_by_release_date_ties = Window.orderBy(F.col("release_date").asc_nulls_last())
w_artist_id_by_duration_sec_mi_3 = Window.partitionBy("artist_id").orderBy("duration_sec_minus_bpm", "id").rowsBetween(-6, -1)

valid_tracks = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

windowed = (
    valid_tracks
    .select(
        "id",
        "title",
        "is_explicit",
        "duration_sec_minus_bpm",
        F.round(F.percent_rank().over(w_artist_id_by_duration_sec_mi), 3).alias("duration_sec_minus_bpm_percent_rank"),
        F.lead("bpm").over(w_artist_id_by_duration_sec_mi_2).alias("next_bpm"),
        F.round(F.avg("duration_sec_minus_bpm").over(w_is_explicit), 2).alias("is_explicit_avg_duration_sec_minus_bpm"),
        F.first("bpm", ignorenulls=True).over(w_artist_id_is_explicit_by_bpm).alias("first_known_bpm"),
        F.round(F.cume_dist().over(w_artist_id_by_duration_sec_mi_2), 3).alias("duration_sec_minus_bpm_cume_dist"),
        F.lag("release_date").over(w_artist_id_by_duration_sec_mi_2).alias("prev_release_date"),
        F.sum("duration_sec_minus_bpm").over(w_is_explicit_release_date_mon).alias("is_explicit_release_date_month_sum_duration_sec_"),
        F.sum("duration_sec_minus_bpm").over(w_is_explicit).alias("is_explicit_sum_duration_sec_minus_bpm"),
        F.sum("bpm").over(w_all_by_release_date_running).alias("running_bpm"),
        F.dense_rank().over(w_artist_id_is_explicit_by_bpm_2).alias("bpm_dense_rank"),
        F.dense_rank().over(w_all_by_release_date_ties).alias("release_date_dense_rank"),
        F.last("release_date", ignorenulls=True).over(w_artist_id_is_explicit_by_bpm).alias("last_known_release_date"),
        F.round(F.avg("duration_sec_minus_bpm").over(w_artist_id_by_duration_sec_mi_3), 2).alias("duration_sec_minus_bpm_avg_6_prior"),
    )
)

enriched = (
    windowed
    .filter(F.col("running_bpm") <= 1191)
    .select(
        "*",
        (F.floor(F.col("first_known_bpm") / 25) * 25).alias("first_known_bpm_bucket"),
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
    )
)

result = (
    enriched
    .filter(F.col("prev_release_date") < F.lit("2025-03-29").cast("date"))
    .select(
        "id",
        "title",
        "is_explicit",
        "duration_sec_minus_bpm",
        "duration_sec_minus_bpm_percent_rank",
        "is_explicit_avg_duration_sec_minus_bpm",
        "first_known_bpm",
        "duration_sec_minus_bpm_cume_dist",
        "prev_release_date",
        "is_explicit_sum_duration_sec_minus_bpm",
        "running_bpm",
        "bpm_dense_rank",
        "release_date_dense_rank",
        "last_known_release_date",
        "duration_sec_minus_bpm_avg_6_prior",
        "first_known_bpm_bucket",
        F.coalesce(F.col("duration_sec_minus_bpm"), F.lit(0)).alias("duration_sec_minus_bpm_filled"),
        F.when(F.col("bpm_dense_rank") >= 8, "A").when(F.col("bpm_dense_rank") >= 4, "B").otherwise("C").alias("bpm_dense_rank_tier"),
    )
    .orderBy("first_known_bpm", "title")
)
