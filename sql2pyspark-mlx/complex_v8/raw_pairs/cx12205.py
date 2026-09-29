from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_release_date_month = Window.partitionBy("artist_id", F.month("release_date"))
w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id")
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id")
w_is_explicit_artist_id_by_rel_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_release_date_month_is_explic = Window.partitionBy(F.month("release_date"), "is_explicit")
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec", "id").rowsBetween(-1, Window.currentRow)
w_is_explicit_artist_id_by_rel_3 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_is_explicit_by_dur_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec")
w_seq_no_bpm_rank_by_duration = Window.partitionBy("seq_no", "bpm_rank").orderBy(F.col("duration_sec_sum_4").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_bpm_rank_release_date_plus_3 = Window.partitionBy("bpm_rank", F.year("release_date_plus_3m"))
w_release_date_plus_3m_month_b = Window.partitionBy(F.date_trunc("month", "release_date_plus_3m")).orderBy("artist_id_release_date_month_min_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_bpm_rank_by_seq_no = Window.partitionBy("bpm_rank").orderBy("seq_no", "id")
w_seq_no_bpm_rank_by_duration_2 = Window.partitionBy("seq_no", "bpm_rank").orderBy(F.col("duration_sec_sum_4").asc_nulls_last())

scoped_tracks = (
    df_tracks
    .filter(F.col("artist_id") >= 9)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.add_months(F.col("release_date"), 3).alias("release_date_plus_3m"),
    )
)

ranked = (
    scoped_tracks
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "release_date_plus_3m",
        F.min("bpm").over(w_artist_id_release_date_month).alias("artist_id_release_date_month_min_bpm"),
        F.lag("duration_sec").over(w_is_explicit_artist_id_by_rel).alias("prev_duration_sec"),
        F.rank().over(w_artist_id_by_bpm).alias("bpm_rank"),
        F.sum("duration_sec").over(w_is_explicit_artist_id_by_rel_2).alias("duration_sec_sum_4"),
        F.row_number().over(w_is_explicit_artist_id_by_rel).alias("seq_no"),
        F.min("bpm").over(w_release_date_month_is_explic).alias("release_date_month_is_explicit_min_bpm"),
        F.round(F.avg("duration_sec").over(w_artist_id_is_explicit_by_dur), 2).alias("duration_sec_avg_2"),
        F.last("duration_sec").over(w_is_explicit_artist_id_by_rel_3).alias("last_duration_sec"),
        F.round(F.percent_rank().over(w_artist_id_is_explicit_by_dur_2), 3).alias("duration_sec_percent_rank"),
    )
)

metrics = (
    ranked
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date_plus_3m",
        "seq_no",
        "release_date_month_is_explicit_min_bpm",
        "last_duration_sec",
        "duration_sec_percent_rank",
        F.last("release_date").over(w_seq_no_bpm_rank_by_duration).alias("last_release_date"),
        F.count(F.lit(1)).over(w_bpm_rank_release_date_plus_3).alias("rows_in_bpm_rank_release_date_plus_3m_year"),
        F.nth_value("last_duration_sec", 2).over(w_release_date_plus_3m_month_b).alias("second_last_duration_sec"),
        F.lag("duration_sec_percent_rank", 3).over(w_bpm_rank_by_seq_no).alias("prev3_duration_sec_percent_rank"),
        F.round(F.cume_dist().over(w_seq_no_bpm_rank_by_duration_2), 3).alias("duration_sec_sum_4_cume_dist"),
        F.round(F.percent_rank().over(w_seq_no_bpm_rank_by_duration_2), 3).alias("duration_sec_sum_4_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "duration_sec",
        "release_date_plus_3m",
        "seq_no",
        "last_duration_sec",
        "rows_in_bpm_rank_release_date_plus_3m_year",
        "second_last_duration_sec",
        "prev3_duration_sec_percent_rank",
        "duration_sec_sum_4_cume_dist",
        "duration_sec_sum_4_percent_rank",
        F.round(F.col("last_duration_sec") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("last_duration_sec_per_duration_sec"),
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_ref"),
    )
    .orderBy(
        F.col("release_date_plus_3m").desc(),
        F.col("second_last_duration_sec").asc_nulls_last(),
    )
)
