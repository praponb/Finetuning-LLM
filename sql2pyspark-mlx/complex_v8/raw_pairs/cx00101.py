from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec", "id")
w_all_by_release_date_full = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date"))
w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))
w_all_by_release_date = Window.orderBy(F.col("release_date").desc(), "id")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy("duration_sec")
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm", "id")
w_artist_id_is_explicit_by_bpm_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_id_by_dur_2 = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_artist_id_release_dat = Window.orderBy("artist_id_release_date_month_avg_duration_sec", "id").rowsBetween(-3, Window.currentRow)
w_distinct_artist_id_in_is_exp = Window.partitionBy("distinct_artist_id_in_is_explicit").orderBy("duration_sec_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_artist_id_in_is_exp_2 = Window.partitionBy("distinct_artist_id_in_is_explicit").orderBy("duration_sec_cume_dist", "id")

cleaned_tracks = (
    df_tracks
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
    cleaned_tracks
    .select(
        "*",
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_is_explicit_artist_id_by_dur)).alias("days_since_prev_release_date"),
        F.nth_value("duration_sec", 2).over(w_all_by_release_date_full).alias("second_duration_sec"),
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.round(F.avg("duration_sec").over(w_artist_id_release_date_month), 2).alias("artist_id_release_date_month_avg_duration_sec"),
        F.sum("bpm").over(w_release_date_month).alias("release_date_month_sum_bpm"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_all_by_release_date)).alias("days_since_prev_release_date_2"),
        F.nth_value("bpm", 2).over(w_is_explicit_by_duration_sec).alias("second_bpm"),
        F.rank().over(w_is_explicit_by_duration_sec_2).alias("duration_sec_rank"),
        F.round(F.avg("duration_sec").over(w_release_date_month), 2).alias("release_date_month_avg_duration_sec"),
        F.round(F.cume_dist().over(w_artist_id_is_explicit_by_bpm), 3).alias("bpm_cume_dist"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit_by_bpm_2).alias("running_count"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_release_date_month), F.lit(0)), 4).alias("duration_sec_share_of_release_date_month"),
        F.round(F.cume_dist().over(w_is_explicit_artist_id_by_dur_2), 3).alias("duration_sec_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_release_date), 3).alias("release_date_percent_rank"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.max("second_bpm").over(w_all_by_artist_id_release_dat).alias("second_bpm_max_4"),
        F.sum("release_date_percent_rank").over(w_distinct_artist_id_in_is_exp).alias("running_release_date_percent_rank"),
        F.row_number().over(w_distinct_artist_id_in_is_exp_2).alias("duration_sec_cume_dist_seq"),
        F.round(F.col("duration_sec_rank") / F.nullif(F.sum("duration_sec_rank").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_rank_share_of_artist_id"),
        F.min("duration_sec_cume_dist").over(w_artist_id).alias("artist_id_min_duration_sec_cume_dist"),
        F.min("duration_sec_rank").over(w_artist_id).alias("artist_id_min_duration_sec_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "days_since_prev_release_date",
        "second_duration_sec",
        "distinct_artist_id_in_is_explicit",
        "artist_id_release_date_month_avg_duration_sec",
        "release_date_month_sum_bpm",
        "days_since_prev_release_date_2",
        "duration_sec_rank",
        "release_date_month_avg_duration_sec",
        "duration_sec_share_of_release_date_month",
        "duration_sec_cume_dist",
        "release_date_percent_rank",
        "second_bpm_max_4",
        "duration_sec_cume_dist_seq",
        "duration_sec_rank_share_of_artist_id",
        "artist_id_min_duration_sec_cume_dist",
        "artist_id_min_duration_sec_rank",
        F.when(F.col("second_duration_sec") >= 353, "A").when(F.col("second_duration_sec") >= 184, "B").otherwise("C").alias("second_duration_sec_tier"),
    )
    .orderBy(
        "id",
        F.col("duration_sec_share_of_release_date_month").desc_nulls_first(),
    )
    .limit(5)
)
