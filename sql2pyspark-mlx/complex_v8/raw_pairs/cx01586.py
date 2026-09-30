from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_release_date_dayofweek_is_ex = Window.partitionBy("release_date_dayofweek", "is_explicit")
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id")
w_release_date_month_artist_id = Window.partitionBy(F.month("release_date"), "artist_id")
w_all_by_duration_sec_prior_6 = Window.orderBy("duration_sec", "id").rowsBetween(-6, -1)
w_all_by_duration_sec_full = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm_moving_1 = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(-1, Window.currentRow)
w_is_explicit_release_date_day = Window.partitionBy("is_explicit", "release_date_dayofweek").orderBy(F.col("duration_sec").desc())

scoped = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-08-02").cast("date")) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
    )
)

windowed = (
    scoped
    .select(
        "*",
        F.ntile(3).over(w_all_by_duration_sec).alias("duration_sec_tercile"),
        F.row_number().over(w_all_by_duration_sec).alias("rn"),
        F.size(F.collect_set("artist_id").over(w_release_date_dayofweek_is_ex)).alias("distinct_artist_id_in_release_date_dayofweek_is_"),
        F.count(F.lit(1)).over(w_release_date_dayofweek_is_ex).alias("rows_in_release_date_dayofweek_is_explicit"),
        F.round(F.cume_dist().over(w_artist_id_by_bpm), 3).alias("bpm_cume_dist"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_release_date_month_artist_id), F.lit(0)), 4).alias("bpm_share_of_release_date_month_artist_id"),
        F.max("release_date_dayofweek").over(w_all_by_duration_sec_prior_6).alias("release_date_dayofweek_max_6_prior"),
        F.nth_value("release_date_dayofweek", 2).over(w_all_by_duration_sec_full).alias("second_release_date_dayofweek"),
        F.round(F.avg("bpm").over(w_release_date_dayofweek_is_ex), 2).alias("release_date_dayofweek_is_explicit_avg_bpm"),
        F.sum("release_date_dayofweek").over(w_artist_id_by_bpm_moving_1).alias("release_date_dayofweek_sum_2"),
        F.max("bpm").over(w_release_date_month_artist_id).alias("release_date_month_artist_id_max_bpm"),
        F.round(F.percent_rank().over(w_is_explicit_release_date_day), 3).alias("duration_sec_percent_rank"),
        F.round(F.cume_dist().over(w_is_explicit_release_date_day), 3).alias("duration_sec_cume_dist"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_release_date_month_artist_id), 2).alias("duration_sec_vs_release_date_month_artist_id_avg"),
        F.min("duration_sec").over(w_release_date_month_artist_id).alias("release_date_month_artist_id_min_duration_sec"),
    )
)

result = (
    windowed
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "rn",
        "distinct_artist_id_in_release_date_dayofweek_is_",
        "bpm_cume_dist",
        "bpm_share_of_release_date_month_artist_id",
        "release_date_dayofweek_max_6_prior",
        "second_release_date_dayofweek",
        "release_date_dayofweek_is_explicit_avg_bpm",
        "release_date_dayofweek_sum_2",
        "release_date_month_artist_id_max_bpm",
        "duration_sec_percent_rank",
        "duration_sec_cume_dist",
        "duration_sec_vs_release_date_month_artist_id_avg",
        "release_date_month_artist_id_min_duration_sec",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label_2"),
    )
    .orderBy("release_date_dayofweek_max_6_prior")
)
