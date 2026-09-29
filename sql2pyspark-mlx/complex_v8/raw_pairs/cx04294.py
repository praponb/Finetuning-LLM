from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm = Window.orderBy("bpm", "id")
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_week_month_by_r = Window.partitionBy(F.month("release_date_week")).orderBy("release_date_week", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_is_explicit_by_bpm_full = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_release_date_wee = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date_week")).orderBy("release_date_week", "id")
w_is_explicit_by_bpm_moving_3 = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id").rowsBetween(-3, Window.currentRow)
w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date_week").desc(), "id")
w_is_explicit = Window.partitionBy("is_explicit")
w_release_date_week_year_artis = Window.partitionBy(F.year("release_date_week"), "artist_id").orderBy(F.col("next3_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prev_release_date_week_month = Window.partitionBy(F.date_trunc("month", "prev_release_date_week"))
w_release_date_week_year_artis_2 = Window.partitionBy(F.year("release_date_week"), "artist_id").orderBy(F.col("next3_duration_sec").desc(), "id").rowsBetween(-2, -1)

prepared_tracks = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-08-26").cast("date"), F.lit("2025-08-02").cast("date"))) & (F.col("duration_sec").between(184, 353)))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
    )
)

ranked = (
    prepared_tracks
    .select(
        "*",
        F.lag("release_date_week").over(w_all_by_bpm).alias("prev_release_date_week"),
        F.first("duration_sec").over(w_artist_id_is_explicit_by_dur).alias("first_duration_sec"),
        F.nth_value("duration_sec", 2).over(w_release_date_week_month_by_r).alias("second_duration_sec"),
        F.lag("bpm").over(w_is_explicit_by_bpm).alias("prev_bpm"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_is_explicit_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_is_explicit_artist_id"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit_artist_id), F.lit(0)), 4).alias("bpm_share_of_is_explicit_artist_id"),
        F.lead("duration_sec", 3, 0).over(w_is_explicit_by_bpm).alias("next3_duration_sec"),
        F.nth_value("duration_sec", 2).over(w_is_explicit_by_bpm_full).alias("second_duration_sec_2"),
        F.round((F.unix_timestamp("release_date_week") - F.unix_timestamp(F.lag("release_date_week").over(w_is_explicit_by_bpm))) / 60, 1).alias("minutes_since_prev_release_date_week"),
        F.lag("duration_sec", 2, 0).over(w_is_explicit_release_date_wee).alias("prev2_duration_sec"),
        F.sum("bpm").over(w_is_explicit_by_bpm_moving_3).alias("bpm_sum_4"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.round((F.unix_timestamp("prev_release_date_week") - F.unix_timestamp(F.lag("prev_release_date_week").over(w_is_explicit_artist_id_by_rel))) / 60, 1).alias("minutes_since_prev_prev_release_date_week"),
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.last("second_duration_sec_2").over(w_release_date_week_year_artis).alias("last_second_duration_sec_2"),
        F.round(F.avg("first_duration_sec").over(w_prev_release_date_week_month), 2).alias("prev_release_date_week_month_avg_first_duration_"),
        F.lag("second_duration_sec", 1, 0).over(w_is_explicit_artist_id_by_rel).alias("prev_second_duration_sec"),
        F.size(F.collect_set("artist_id").over(w_prev_release_date_week_month)).alias("distinct_artist_id_in_prev_release_date_week_mon"),
        F.round(F.avg("prev2_duration_sec").over(w_release_date_week_year_artis_2), 2).alias("prev2_duration_sec_avg_2_prior"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "moods",
        "prev_release_date_week",
        "first_duration_sec",
        "prev_bpm",
        "duration_sec_share_of_is_explicit_artist_id",
        "bpm_share_of_is_explicit_artist_id",
        "second_duration_sec_2",
        "minutes_since_prev_release_date_week",
        "prev2_duration_sec",
        "bpm_sum_4",
        "last_second_duration_sec_2",
        "prev_release_date_week_month_avg_first_duration_",
        "prev_second_duration_sec",
        "distinct_artist_id_in_prev_release_date_week_mon",
        F.least(F.col("prev_second_duration_sec"), F.col("second_duration_sec_2")).alias("least_prev_second_duration_sec_second_duration_s"),
    )
)
