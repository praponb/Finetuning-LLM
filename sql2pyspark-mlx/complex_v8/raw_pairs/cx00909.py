from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_by_release_date_fu = Window.partitionBy("artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_release_date_month_artist_id = Window.partitionBy(F.month("release_date"), "artist_id")
w_artist_id_by_release_date_pr = Window.partitionBy("artist_id").orderBy("release_date", "id").rowsBetween(-6, -1)
w_artist_id_by_release_date_fu_2 = Window.partitionBy("artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_release_date_plu = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date_plus_6m")).orderBy("release_date_month_end", "id").rowsBetween(-1, -1)
w_is_explicit_release_date_plu_2 = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date_plus_6m")).orderBy("release_date_month_end", "id")
w_release_date_plus_6m_month_b = Window.partitionBy(F.month("release_date_plus_6m")).orderBy("second_bpm", "id").rowsBetween(-1, -1)
w_artist_id_release_date_plus = Window.partitionBy("artist_id", F.year("release_date_plus_6m"))
w_is_explicit_release_date_plu_3 = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date_plus_6m")).orderBy("release_date_month_end", "id").rowsBetween(-3, Window.currentRow)
w_all_by_second_bpm_running = Window.orderBy(F.col("second_bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_release_date_plu_4 = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date_plus_6m")).orderBy("release_date_month_end")

cleaned_tracks = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-09-26").cast("date"), F.lit("2025-08-07").cast("date"))) & (F.col("artist_id") >= 11))
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

windowed = (
    cleaned_tracks
    .select(
        "*",
        F.sum("duration_sec").over(w_is_explicit).alias("is_explicit_sum_duration_sec"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.nth_value("bpm", 2).over(w_artist_id_by_release_date_fu).alias("second_bpm"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_all_by_duration_sec)).alias("duration_sec_change"),
        F.min("bpm").over(w_release_date_month_artist_id).alias("release_date_month_artist_id_min_bpm"),
        F.max("duration_sec").over(w_artist_id_by_release_date_pr).alias("duration_sec_max_6_prior"),
        F.round(F.avg("bpm").over(w_is_explicit), 2).alias("is_explicit_avg_bpm"),
        F.max("bpm").over(w_release_date_month_artist_id).alias("release_date_month_artist_id_max_bpm"),
        F.nth_value("duration_sec", 2).over(w_artist_id_by_release_date_fu_2).alias("second_duration_sec"),
    )
)

scored = (
    windowed
    .select(
        "*",
        F.last_day(F.col("release_date")).alias("release_date_month_end"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.add_months(F.col("release_date"), 6).alias("release_date_plus_6m"),
    )
)

with_trends = (
    scored
    .select(
        "*",
        F.sum("bpm").over(w_is_explicit_release_date_plu).alias("bpm_sum_1_prior"),
        F.lead("second_bpm").over(w_is_explicit_release_date_plu_2).alias("next_second_bpm"),
        F.sum("is_explicit_sum_duration_sec").over(w_release_date_plus_6m_month_b).alias("is_explicit_sum_duration_sec_sum_1_prior"),
        F.round(F.avg("second_duration_sec").over(w_is_explicit_release_date_plu_3), 2).alias("second_duration_sec_avg_4"),
        F.last("release_date_month_end", ignorenulls=True).over(w_all_by_second_bpm_running).alias("last_known_release_date_month_end"),
        F.round(F.percent_rank().over(w_is_explicit_release_date_plu_4), 3).alias("release_date_month_end_percent_rank"),
    )
)

summary = (
    with_trends
    .groupBy(
        F.date_trunc("month", "release_date_month_end").alias("release_date_month_end_month"),
        "is_explicit",
    )
    .agg(
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.count(F.when(F.length(F.trim(F.col("title"))) >= 7, 1)).alias("n_length_trim_title_7"),
        F.min("release_date_month_end").alias("first_release_date_month_end"),
        F.count("*").alias("cnt"),
    )
)

result = (
    summary
    .filter(F.col("first_release_date_month_end") >= F.lit("2024-07-01").cast("date"))
    .select(
        "release_date_month_end_month",
        "is_explicit",
        "title_list",
        "n_length_trim_title_7",
        "first_release_date_month_end",
        "cnt",
        F.round(F.col("n_length_trim_title_7") / F.nullif(F.col("cnt"), F.lit(0)), 3).alias("n_length_trim_title_7_per_cnt"),
        (F.floor(F.col("n_length_trim_title_7") / 2) * 2).alias("n_length_trim_title_7_bucket"),
    )
    .orderBy("n_length_trim_title_7", "n_length_trim_title_7_per_cnt")
)
