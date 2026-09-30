from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_bpm = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_id_by_bpm_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_artist_id_by_bpm_3 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").desc(), "id")
w_artist_id_by_bpm_running = Window.partitionBy("artist_id").orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month_by_releas = Window.partitionBy(F.month("release_date")).orderBy(F.col("release_date").desc(), "id")
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.month("release_date")).orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_release_date_month_2 = Window.partitionBy("artist_id", F.month("release_date")).orderBy("bpm", "id")
w_is_explicit_artist_id_by_bpm_4 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").desc())
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_seq_no_bpm_seq_by_second_dur = Window.partitionBy("seq_no", "bpm_seq").orderBy("second_duration_sec", "id")
w_prev_release_date_month = Window.partitionBy(F.month("prev_release_date"))
w_seq_no_by_prev_release_date = Window.partitionBy("seq_no").orderBy(F.col("prev_release_date").desc(), "id")
w_seq_no_bpm_seq_by_bpm = Window.partitionBy("seq_no", "bpm_seq").orderBy(F.col("bpm").desc(), "id")
w_seq_no_bpm_seq_by_bpm_moving = Window.partitionBy("seq_no", "bpm_seq").orderBy(F.col("bpm").desc(), "id").rowsBetween(-3, Window.currentRow)

scoped_tracks = (
    df_tracks
    .filter(F.col("release_date") < F.lit("2025-03-08").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

with_trends = (
    scoped_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.last("bpm", ignorenulls=True).over(w_is_explicit_artist_id_by_bpm).alias("last_known_bpm"),
        F.nth_value("duration_sec", 2).over(w_is_explicit_artist_id_by_bpm_2).alias("second_duration_sec"),
        F.lead("duration_sec").over(w_is_explicit_artist_id_by_bpm_3).alias("next_duration_sec"),
        F.first("release_date", ignorenulls=True).over(w_artist_id_by_bpm_running).alias("first_known_release_date"),
        F.dense_rank().over(w_release_date_month_by_releas).alias("release_date_dense_rank"),
        F.row_number().over(w_is_explicit_artist_id_by_bpm_3).alias("bpm_seq"),
        F.sum("bpm").over(w_artist_id_release_date_month).alias("running_bpm"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_release_date_month_by_releas)).alias("duration_sec_change"),
        F.row_number().over(w_artist_id_release_date_month_2).alias("seq_no"),
        F.lag("release_date").over(w_artist_id_release_date_month_2).alias("prev_release_date"),
        F.rank().over(w_is_explicit_artist_id_by_bpm_4).alias("bpm_rank"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.ntile(5).over(w_release_date_month_by_releas).alias("release_date_quintile"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.round(F.cume_dist().over(w_seq_no_bpm_seq_by_second_dur), 3).alias("second_duration_sec_cume_dist"),
        F.round(F.col("bpm_seq") - F.avg("bpm_seq").over(w_prev_release_date_month), 2).alias("bpm_seq_vs_prev_release_date_month_avg"),
        F.lead("duration_sec_change", 3).over(w_seq_no_by_prev_release_date).alias("next3_duration_sec_change"),
        F.rank().over(w_seq_no_bpm_seq_by_bpm).alias("bpm_rank_2"),
        F.round(F.avg("release_date_quintile").over(w_seq_no_bpm_seq_by_bpm_moving), 2).alias("release_date_quintile_avg_4"),
    )
)

result = (
    ranked
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "last_known_bpm",
        "second_duration_sec",
        "next_duration_sec",
        "first_known_release_date",
        "bpm_seq",
        "running_bpm",
        "duration_sec_change",
        "seq_no",
        "prev_release_date",
        "bpm_rank",
        "rows_in_is_explicit",
        "release_date_quintile",
        "bpm_seq_vs_prev_release_date_month_avg",
        "next3_duration_sec_change",
        "bpm_rank_2",
        "release_date_quintile_avg_4",
        F.concat(F.col("title"), F.lit("#"), F.col("rows_in_is_explicit").cast("string")).alias("title_ref"),
    )
    .orderBy("release_date", F.col("bpm_seq_vs_prev_release_date_month_avg").desc())
)
