from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date = Window.orderBy("release_date", "id")
w_all_by_release_date_ties = Window.orderBy("release_date")
w_release_date_month_artist_is = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_is_verified")
w_all_by_release_date_running = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified_by_least = Window.partitionBy("artist_is_verified").orderBy("least_duration_sec_bpm", "id")
w_artist_is_verified_by_least_2 = Window.partitionBy("artist_is_verified").orderBy("least_duration_sec_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date_full = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id").orderBy(F.col("least_duration_sec_bpm").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_artist_is_verified_artist_id_2 = Window.partitionBy("artist_is_verified", "artist_id").orderBy(F.col("least_duration_sec_bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_genre_rn = Window.partitionBy("artist_genre", "rn")
w_release_date_rank_2_by_runni = Window.partitionBy("release_date_rank_2").orderBy("running_least_duration_sec_bpm", "id")
w_release_date_rank_least_dura = Window.partitionBy("release_date_rank", "least_duration_sec_bpm_rank").orderBy("last_release_date", "id")
w_release_date_rank_2_by_runni_2 = Window.partitionBy("release_date_rank_2").orderBy("running_least_duration_sec_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn = Window.partitionBy("rn")
w_all_by_rn = Window.orderBy("rn", "id")

cleaned_tracks = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 2) & (F.col("duration_sec") >= 154))
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
        F.least(F.col("duration_sec"), F.col("bpm")).alias("least_duration_sec_bpm"),
    )
)

joined = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.least_duration_sec_bpm",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.rank().over(w_all_by_release_date).alias("release_date_rank"),
        F.rank().over(w_all_by_release_date_ties).alias("release_date_rank_2"),
        F.count(F.lit(1)).over(w_release_date_month_artist_is).alias("rows_in_release_date_month_artist_is_verified"),
        F.sum("least_duration_sec_bpm").over(w_all_by_release_date_running).alias("running_least_duration_sec_bpm"),
        F.min("least_duration_sec_bpm").over(w_release_date_month_artist_is).alias("release_date_month_artist_is_verified_min_least_"),
        F.rank().over(w_artist_is_verified_by_least).alias("least_duration_sec_bpm_rank"),
        F.last("release_date").over(w_artist_is_verified_by_least_2).alias("last_release_date"),
        F.last("bpm").over(w_all_by_release_date_full).alias("last_bpm"),
        F.nth_value("bpm", 2).over(w_artist_is_verified_by_least_2).alias("second_bpm"),
        F.max("bpm").over(w_artist_is_verified_artist_id).alias("bpm_max_1_prior"),
        F.first("release_date", ignorenulls=True).over(w_artist_is_verified_artist_id_2).alias("first_known_release_date"),
        F.row_number().over(w_artist_is_verified_by_least).alias("rn"),
        F.last("release_date", ignorenulls=True).over(w_all_by_release_date_running).alias("last_known_release_date"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.sum("second_bpm").over(w_artist_genre_rn).alias("artist_genre_rn_sum_second_bpm"),
        F.lead("release_date").over(w_release_date_rank_2_by_runni).alias("next_release_date"),
        F.datediff(F.col("last_release_date"), F.lag("last_release_date").over(w_release_date_rank_least_dura)).alias("days_since_prev_last_release_date"),
        F.nth_value("release_date_rank_2", 2).over(w_release_date_rank_2_by_runni_2).alias("second_release_date_rank_2"),
        F.round(F.col("least_duration_sec_bpm") / F.nullif(F.sum("least_duration_sec_bpm").over(w_rn), F.lit(0)), 4).alias("least_duration_sec_bpm_share_of_rn"),
        F.lag("bpm_max_1_prior", 1, 0).over(w_all_by_rn).alias("prev_bpm_max_1_prior"),
        F.count(F.lit(1)).over(w_artist_genre_rn).alias("rows_in_artist_genre_rn"),
    )
)

result = (
    windowed
    .filter(F.col("days_since_prev_last_release_date") < 170)
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "least_duration_sec_bpm",
        "artist_genre",
        "release_date_rank",
        "release_date_rank_2",
        "rows_in_release_date_month_artist_is_verified",
        "running_least_duration_sec_bpm",
        "release_date_month_artist_is_verified_min_least_",
        "least_duration_sec_bpm_rank",
        "second_bpm",
        "bpm_max_1_prior",
        "last_known_release_date",
        "next_release_date",
        "second_release_date_rank_2",
        "least_duration_sec_bpm_share_of_rn",
        "prev_bpm_max_1_prior",
        "rows_in_artist_genre_rn",
    )
    .orderBy(
        F.col("rows_in_release_date_month_artist_is_verified").desc(),
        F.col("bpm_max_1_prior").desc(),
    )
)
