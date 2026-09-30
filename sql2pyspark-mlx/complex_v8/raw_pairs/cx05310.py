from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm_minus_duration_se = Window.orderBy(F.col("bpm_minus_duration_sec").desc(), "id").rowsBetween(-3, -1)
w_all_by_bpm_minus_duration_se_2 = Window.orderBy(F.col("bpm_minus_duration_sec").desc())
w_artist_id = Window.partitionBy("artist_id")
w_all_by_bpm_minus_duration_se_3 = Window.orderBy("bpm_minus_duration_sec", "id").rowsBetween(-6, Window.currentRow)
w_all_by_bpm_minus_duration_se_4 = Window.orderBy("bpm_minus_duration_sec", "id")
w_all_by_bpm_minus_duration_se_5 = Window.orderBy(F.col("bpm_minus_duration_sec").desc(), "id")
w_all_by_bpm_minus_duration_se_6 = Window.orderBy(F.col("bpm_minus_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm_minus_duration_se_7 = Window.orderBy(F.col("bpm_minus_duration_sec").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_bpm_minus_duration_se_8 = Window.orderBy(F.col("bpm_minus_duration_sec").desc(), "id").rowsBetween(-2, -1)

base_tracks = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "focus")) & (F.lower(F.col("title")).like("%lo%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
    )
)

ranked = (
    base_tracks
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods_text",
        "bpm_minus_duration_sec",
        F.round(F.avg("duration_sec").over(w_all_by_bpm_minus_duration_se), 2).alias("duration_sec_avg_3_prior"),
        F.round(F.cume_dist().over(w_all_by_bpm_minus_duration_se_2), 3).alias("bpm_minus_duration_sec_cume_dist"),
        F.sum("duration_sec").over(w_artist_id).alias("artist_id_sum_duration_sec"),
        F.round(F.avg("bpm_minus_duration_sec").over(w_all_by_bpm_minus_duration_se_3), 2).alias("bpm_minus_duration_sec_avg_7"),
        F.round(F.cume_dist().over(w_all_by_bpm_minus_duration_se_4), 3).alias("bpm_minus_duration_sec_cume_dist_2"),
        F.round(F.cume_dist().over(w_all_by_bpm_minus_duration_se_5), 3).alias("bpm_minus_duration_sec_cume_dist_3"),
        F.lead("duration_sec", 2, 0).over(w_all_by_bpm_minus_duration_se_5).alias("next2_duration_sec"),
        F.round(F.percent_rank().over(w_all_by_bpm_minus_duration_se_2), 3).alias("bpm_minus_duration_sec_percent_rank"),
        F.count(F.lit(1)).over(w_all_by_bpm_minus_duration_se_6).alias("running_count"),
        F.sum("duration_sec").over(w_all_by_bpm_minus_duration_se_7).alias("duration_sec_sum_4"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        F.round(F.col("bpm_minus_duration_sec") / F.nullif(F.sum("bpm_minus_duration_sec").over(w_artist_id), F.lit(0)), 4).alias("bpm_minus_duration_sec_share_of_artist_id"),
        F.round(F.avg("duration_sec").over(w_all_by_bpm_minus_duration_se_8), 2).alias("duration_sec_avg_2_prior"),
        F.lead("bpm_minus_duration_sec").over(w_all_by_bpm_minus_duration_se_4).alias("next_bpm_minus_duration_sec"),
    )
)

stats = (
    ranked
    .groupBy(
        "is_explicit",
        F.date_trunc("month", "release_date").alias("release_date_month"),
    )
    .agg(
        F.round(F.sum("duration_sec_avg_3_prior"), 2).alias("total_duration_sec_avg_3_prior"),
        F.percentile_approx("next_bpm_minus_duration_sec", 0.5).alias("median_next_bpm_minus_duration_sec"),
        F.max("release_date").alias("last_release_date"),
        F.round(F.sum("bpm_minus_duration_sec_cume_dist_3"), 2).alias("total_bpm_minus_duration_sec_cume_dist_3"),
        F.sum(F.when(F.upper(F.trim(F.col("title"))).like("G%"), F.col("duration_sec_avg_2_prior")).otherwise(0)).alias("duration_sec_avg_2_prior_upper_trim_title_like_g"),
        F.percentile_approx("bpm_minus_duration_sec_percent_rank", 0.5).alias("median_bpm_minus_duration_sec_percent_rank"),
    )
)

result = (
    stats
    .select(
        "is_explicit",
        "last_release_date",
        "median_bpm_minus_duration_sec_percent_rank",
        F.dayofweek(F.col("release_date_month")).alias("release_date_month_dayofweek"),
        F.last_day(F.col("last_release_date")).alias("last_release_date_month_end"),
    )
    .orderBy(
        "median_bpm_minus_duration_sec_percent_rank",
        "release_date_month_dayofweek",
    )
)
