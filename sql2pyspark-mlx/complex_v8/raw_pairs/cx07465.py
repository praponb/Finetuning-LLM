from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_title_len_full = Window.orderBy(F.col("title_len").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_title_len_running = Window.orderBy(F.col("title_len").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_title_len_artist_is_verified = Window.partitionBy("title_len", "artist_is_verified").orderBy(F.col("title_len").desc(), "id").rowsBetween(-2, Window.currentRow)
w_is_explicit_artist_is_verifi = Window.partitionBy("is_explicit", "artist_is_verified").orderBy("bpm", "id").rowsBetween(-3, Window.currentRow)
w_is_explicit_artist_is_verifi_2 = Window.partitionBy("is_explicit", "artist_is_verified").orderBy("bpm", "id")
w_artist_is_verified_by_title = Window.partitionBy("artist_is_verified").orderBy(F.col("title_len").desc(), "id").rowsBetween(-6, Window.currentRow)
w_is_explicit_artist_is_verifi_3 = Window.partitionBy("is_explicit", "artist_is_verified").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified = Window.partitionBy("artist_is_verified")
w_is_explicit_artist_is_verifi_4 = Window.partitionBy("is_explicit", "artist_is_verified").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_title_len_artist_is_verified_2 = Window.partitionBy("title_len", "artist_is_verified").orderBy(F.col("title_len").desc())
w_artist_is_verified_by_title_2 = Window.partitionBy("artist_is_verified").orderBy(F.col("title_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_sum_7 = Window.orderBy("bpm_sum_7", "id")
w_artist_is_verified_artist_ge = Window.partitionBy("artist_is_verified", "artist_genre")

valid = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

combined_details = (
    valid.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre") == "Pop")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.last("release_date").over(w_all_by_title_len_full).alias("last_release_date"),
        F.count(F.lit(1)).over(w_all_by_title_len_running).alias("running_count"),
        F.round(F.col("title_len") / F.nullif(F.sum("title_len").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("title_len_share_of_artist_id_is_explicit"),
        F.round(F.avg("bpm").over(w_title_len_artist_is_verified), 2).alias("bpm_avg_3"),
        F.round(F.avg("title_len").over(w_is_explicit_artist_is_verifi), 2).alias("title_len_avg_4"),
        F.first("bpm", ignorenulls=True).over(w_all_by_title_len_running).alias("first_known_bpm"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_is_explicit_artist_is_verifi_2)).alias("days_since_prev_release_date"),
        F.size(F.collect_set("artist_genre").over(w_artist_id_is_explicit)).alias("distinct_artist_genre_in_artist_id_is_explicit"),
        F.sum("bpm").over(w_artist_is_verified_by_title).alias("bpm_sum_7"),
        F.first("title_len", ignorenulls=True).over(w_is_explicit_artist_is_verifi_3).alias("first_known_title_len"),
        F.max("title_len").over(w_artist_is_verified).alias("artist_is_verified_max_title_len"),
        F.row_number().over(w_is_explicit_artist_is_verifi_2).alias("seq_no"),
        F.nth_value("release_date", 2).over(w_is_explicit_artist_is_verifi_4).alias("second_release_date"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_is_verified), F.lit(0)), 4).alias("bpm_share_of_artist_is_verified"),
        F.rank().over(w_title_len_artist_is_verified_2).alias("title_len_rank"),
        F.nth_value("bpm", 2).over(w_artist_is_verified_by_title_2).alias("second_bpm"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_bpm_sum_7), 3).alias("bpm_sum_7_cume_dist"),
        F.size(F.collect_set("artist_id").over(w_artist_is_verified_artist_ge)).alias("distinct_artist_id_in_artist_is_verified_artist_"),
        F.lag("last_release_date").over(w_all_by_bpm_sum_7).alias("prev_last_release_date"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "release_date",
        "is_explicit",
        "bpm",
        "title_len",
        "artist_genre",
        "last_release_date",
        "running_count",
        "title_len_share_of_artist_id_is_explicit",
        "bpm_avg_3",
        "title_len_avg_4",
        "first_known_bpm",
        "bpm_sum_7",
        "first_known_title_len",
        "seq_no",
        "second_release_date",
        "bpm_share_of_artist_is_verified",
        "title_len_rank",
        "second_bpm",
        "bpm_sum_7_cume_dist",
        "distinct_artist_id_in_artist_is_verified_artist_",
        "prev_last_release_date",
        F.datediff(F.col("release_date"), F.col("second_release_date")).alias("days_second_release_date_to_release_date"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("prev_last_release_date")).alias("days_since_prev_last_release_date"),
    )
    .orderBy(F.col("bpm_sum_7").asc(), "release_date", "id")
    .limit(10)
)
