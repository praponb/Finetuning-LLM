from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id")
w_all_by_bpm_moving_1 = Window.orderBy("bpm", "id").rowsBetween(-1, Window.currentRow)
w_release_date_month_artist_is = Window.partitionBy(F.month("release_date"), "artist_is_verified").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_artist_is_2 = Window.partitionBy(F.month("release_date"), "artist_is_verified").orderBy("release_date", "id")
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_artist_is_verified = Window.partitionBy("artist_is_verified")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_artist_is_verified_ar = Window.orderBy(F.col("artist_is_verified_artist_id_min_bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_artist_is_verified_ar_2 = Window.orderBy(F.col("artist_is_verified_artist_id_min_bpm").desc())
w_artist_id_last_release_date = Window.partitionBy("artist_id", F.year("last_release_date")).orderBy("artist_is_verified_max_duration_sec", "id")
w_artist_id_by_release_date_ru = Window.partitionBy("artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_last_release_date_2 = Window.partitionBy("artist_id", F.year("last_release_date")).orderBy("artist_is_verified_max_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm_share_of_ar = Window.partitionBy("artist_id").orderBy(F.col("bpm_share_of_artist_is_verified").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
    )
)

combined_details_artist = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(~F.col("a.is_verified"))
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
    )
)

sequenced = (
    combined_details_artist
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_cume_dist"),
        F.first("duration_sec", ignorenulls=True).over(w_all_by_duration_sec_running).alias("first_known_duration_sec"),
        F.min("bpm").over(w_artist_is_verified_artist_id).alias("artist_is_verified_artist_id_min_bpm"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_is_verified_artist_id), 2).alias("duration_sec_vs_artist_is_verified_artist_id_avg"),
        F.round(F.avg("bpm").over(w_all_by_bpm_moving_1), 2).alias("bpm_avg_2"),
        F.last("release_date").over(w_release_date_month_artist_is).alias("last_release_date"),
        F.min("duration_sec").over(w_artist_is_verified_artist_id).alias("artist_is_verified_artist_id_min_duration_sec"),
        F.row_number().over(w_release_date_month_artist_is_2).alias("seq_no"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_all_by_duration_sec)).alias("duration_sec_change"),
        F.max("duration_sec").over(w_artist_is_verified).alias("artist_is_verified_max_duration_sec"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_is_verified), F.lit(0)), 4).alias("bpm_share_of_artist_is_verified"),
    )
)

top_per_group = (
    sequenced
    .filter((F.col("seq_no") <= 5) & (F.length(F.trim(F.col("artist_name"))) >= 4))
    .drop("seq_no")
)

with_windows = (
    top_per_group
    .select(
        "*",
        F.first("first_known_duration_sec", ignorenulls=True).over(w_all_by_artist_is_verified_ar).alias("first_known_first_known_duration_sec"),
        F.round(F.cume_dist().over(w_all_by_artist_is_verified_ar_2), 3).alias("artist_is_verified_artist_id_min_bpm_cume_dist"),
        F.rank().over(w_artist_id_last_release_date).alias("artist_is_verified_max_duration_sec_rank"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
        F.first("last_release_date", ignorenulls=True).over(w_artist_id_by_release_date_ru).alias("first_known_last_release_date"),
        F.last("bpm").over(w_artist_id_last_release_date_2).alias("last_bpm"),
        F.nth_value("duration_sec", 2).over(w_artist_id_by_bpm_share_of_ar).alias("second_duration_sec"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "artist_name",
        "first_known_duration_sec",
        "duration_sec_vs_artist_is_verified_artist_id_avg",
        "bpm_avg_2",
        "last_release_date",
        "artist_is_verified_artist_id_min_duration_sec",
        "duration_sec_change",
        "artist_is_verified_max_duration_sec",
        "bpm_share_of_artist_is_verified",
        "first_known_first_known_duration_sec",
        "artist_is_verified_artist_id_min_bpm_cume_dist",
        "artist_is_verified_max_duration_sec_rank",
        "duration_sec_share_of_artist_id",
        "last_bpm",
        F.array_contains(F.col("moods"), "focus").alias("has_focus"),
        F.when(F.col("second_duration_sec") >= 357, "large").when(F.col("second_duration_sec") >= 165, "medium").otherwise("small").alias("second_duration_sec_tier"),
    )
)
