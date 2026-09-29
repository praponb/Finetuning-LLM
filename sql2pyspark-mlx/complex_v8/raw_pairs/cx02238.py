from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_title_len_is_explicit_by_tit = Window.partitionBy("title_len", "is_explicit").orderBy("title_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm = Window.orderBy("bpm", "id")
w_release_date_month_by_title = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy("title_len", "id")
w_release_date_month_by_title_2 = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy("title_len", "id").rowsBetween(-6, Window.currentRow)
w_release_date_month_by_title_3 = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy(F.col("title_len").desc_nulls_first(), "id")
w_title_len_artist_id = Window.partitionBy("title_len", "artist_id")
w_release_date_month_by_title_4 = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy("title_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month_by_title_5 = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy(F.col("title_len").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_next2_title_len_release_date = Window.partitionBy("next2_title_len", F.year("release_date")).orderBy(F.col("max_title_len_so_far").desc(), "id")
w_last_known_release_date_mont = Window.partitionBy(F.month("last_known_release_date"))
w_next2_title_len_release_date_2 = Window.partitionBy("next2_title_len", F.year("release_date")).orderBy(F.col("max_title_len_so_far").desc())
w_last_known_release_date_mont_2 = Window.partitionBy(F.month("last_known_release_date")).orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_title_len = Window.partitionBy("artist_id", "title_len")

recent = (
    df_tracks
    .filter(F.col("artist_id") <= 14)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.abs(F.col("bpm") - 142).alias("bpm_distance"),
    )
)

windowed = (
    recent
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "title_len",
        F.last("bpm_distance", ignorenulls=True).over(w_title_len_is_explicit_by_tit).alias("last_known_bpm_distance"),
        F.lag("duration_sec", 2).over(w_all_by_bpm).alias("prev2_duration_sec"),
        F.lead("title_len", 2).over(w_release_date_month_by_title).alias("next2_title_len"),
        F.round(F.avg("title_len").over(w_release_date_month_by_title_2), 2).alias("title_len_avg_7"),
        (F.col("title_len") - F.lag("title_len").over(w_release_date_month_by_title_3)).alias("title_len_change"),
        F.lag("bpm_distance").over(w_release_date_month_by_title).alias("prev_bpm_distance"),
        F.sum("title_len").over(w_title_len_is_explicit_by_tit).alias("running_title_len"),
        F.sum("title_len").over(w_title_len_artist_id).alias("title_len_artist_id_sum_title_len"),
        F.last("release_date", ignorenulls=True).over(w_release_date_month_by_title_4).alias("last_known_release_date"),
        F.max("title_len").over(w_release_date_month_by_title_4).alias("max_title_len_so_far"),
        F.sum("duration_sec").over(w_release_date_month_by_title_5).alias("running_duration_sec"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.round(F.cume_dist().over(w_next2_title_len_release_date), 3).alias("max_title_len_so_far_cume_dist"),
        F.round(F.col("last_known_bpm_distance") - F.avg("last_known_bpm_distance").over(w_last_known_release_date_mont), 2).alias("last_known_bpm_distance_vs_last_known_release_da"),
        F.rank().over(w_next2_title_len_release_date_2).alias("max_title_len_so_far_rank"),
        F.first("max_title_len_so_far", ignorenulls=True).over(w_last_known_release_date_mont_2).alias("first_known_max_title_len_so_far"),
        F.min("next2_title_len").over(w_artist_id_title_len).alias("artist_id_title_len_min_next2_title_len"),
    )
)

result = (
    sequenced
    .filter(F.col("running_duration_sec").between(1323, 3789))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "title_len",
        "last_known_bpm_distance",
        "title_len_avg_7",
        "prev_bpm_distance",
        "last_known_release_date",
        "running_duration_sec",
        "last_known_bpm_distance_vs_last_known_release_da",
        "max_title_len_so_far_rank",
        "first_known_max_title_len_so_far",
        "artist_id_title_len_min_next2_title_len",
        F.year(F.col("last_known_release_date")).alias("last_known_release_date_year"),
    )
    .orderBy(
        F.col("artist_id_title_len_min_next2_title_len").desc_nulls_first(),
        F.col("max_title_len_so_far_rank").desc(),
        "id",
    )
    .limit(5)
)
