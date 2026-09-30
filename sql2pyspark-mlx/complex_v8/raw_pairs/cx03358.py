from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_release_date_yea = Window.partitionBy("is_explicit", F.year("release_date")).orderBy("bpm", "id")
w_is_explicit_release_date_yea_2 = Window.partitionBy("is_explicit", F.year("release_date")).orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm = Window.orderBy("bpm", "id")
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id")
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec")
w_artist_id_release_date_year = Window.partitionBy("artist_id", F.year("release_date"))
w_all_by_bpm_running = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_bpm_moving_3 = Window.orderBy("bpm", "id").rowsBetween(-3, Window.currentRow)
w_all_by_bpm_full = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.month("release_date"))

base = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.col("duration_sec") <= 353))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
    )
)

windowed = (
    base
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "moods",
        F.round(F.cume_dist().over(w_is_explicit_release_date_yea), 3).alias("bpm_cume_dist"),
        F.max("bpm").over(w_is_explicit_release_date_yea_2).alias("max_bpm_so_far"),
        F.row_number().over(w_all_by_bpm).alias("bpm_seq"),
        F.lead("bpm", 2).over(w_artist_id_by_release_date).alias("next2_bpm"),
        F.rank().over(w_artist_id_is_explicit_by_dur).alias("duration_sec_rank"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id_release_date_year), 2).alias("bpm_vs_artist_id_release_date_year_avg"),
        F.last("bpm", ignorenulls=True).over(w_all_by_bpm_running).alias("last_known_bpm"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_id_by_release_date)).alias("duration_sec_change"),
        F.round(F.avg("bpm").over(w_artist_id), 2).alias("artist_id_avg_bpm"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.round(F.avg("bpm").over(w_all_by_bpm_moving_3), 2).alias("bpm_avg_4"),
        F.nth_value("duration_sec", 2).over(w_all_by_bpm_full).alias("second_duration_sec"),
        F.dense_rank().over(w_artist_id_is_explicit_by_dur).alias("duration_sec_dense_rank"),
        F.count(F.lit(1)).over(w_is_explicit_release_date_mon).alias("rows_in_is_explicit_release_date_month"),
    )
)

result = (
    windowed
    .filter(F.col("duration_sec_dense_rank") <= 8)
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "moods",
        "max_bpm_so_far",
        "next2_bpm",
        "bpm_vs_artist_id_release_date_year_avg",
        "last_known_bpm",
        "artist_id_avg_bpm",
        "duration_sec_vs_artist_id_avg",
        "bpm_avg_4",
        "second_duration_sec",
        "duration_sec_dense_rank",
        "rows_in_is_explicit_release_date_month",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
    .orderBy(F.col("is_explicit_label").desc())
)
