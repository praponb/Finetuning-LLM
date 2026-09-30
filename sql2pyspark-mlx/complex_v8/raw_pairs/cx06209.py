from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date_year").desc())
w_all_by_bpm_running = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy(F.col("release_date_year").desc(), "id")
w_release_date_year_is_explici = Window.partitionBy("release_date_year", "is_explicit").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_all_by_bpm = Window.orderBy("bpm", "id")
w_all_by_prev3_release_date_2 = Window.orderBy(F.col("prev3_release_date_2").desc(), "id")
w_duration_sec_decile_by_bpm_f = Window.partitionBy("duration_sec_decile").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_release_date_3 = Window.partitionBy("is_explicit").orderBy(F.col("release_date_year_cume_dist").asc_nulls_last(), "id")
w_is_explicit_by_release_date_4 = Window.partitionBy("is_explicit").orderBy(F.col("release_date_year_cume_dist").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_is_explicit_by_release_date_5 = Window.partitionBy("is_explicit").orderBy(F.col("release_date_year_cume_dist").asc_nulls_last())

cleaned_tracks = (
    df_tracks
    .filter((F.col("is_explicit")) & (F.upper(F.trim(F.col("title"))).like("M%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.year(F.col("release_date")).alias("release_date_year"),
    )
)

ranked = (
    cleaned_tracks
    .select(
        "*",
        F.round(F.cume_dist().over(w_is_explicit_by_release_date), 3).alias("release_date_year_cume_dist"),
        F.sum("bpm").over(w_all_by_bpm_running).alias("running_bpm"),
        F.lead("release_date").over(w_all_by_duration_sec).alias("next_release_date"),
        F.lead("release_date").over(w_is_explicit_by_release_date_2).alias("next_release_date_2"),
        F.ntile(10).over(w_release_date_year_is_explici).alias("duration_sec_decile"),
        F.lag("release_date", 3).over(w_all_by_bpm).alias("prev3_release_date"),
        F.lag("release_date", 3).over(w_is_explicit_by_release_date_2).alias("prev3_release_date_2"),
        F.ntile(3).over(w_release_date_year_is_explici).alias("duration_sec_tercile"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.ntile(5).over(w_all_by_prev3_release_date_2).alias("prev3_release_date_2_quintile"),
        F.nth_value("prev3_release_date", 2).over(w_duration_sec_decile_by_bpm_f).alias("second_prev3_release_date"),
        F.datediff(F.col("next_release_date_2"), F.lag("next_release_date_2").over(w_is_explicit_by_release_date_3)).alias("days_since_prev_next_release_date_2"),
        F.round(F.avg("running_bpm").over(w_is_explicit_by_release_date_4), 2).alias("running_bpm_avg_6_prior"),
        F.lag("duration_sec_decile", 3, 0).over(w_all_by_prev3_release_date_2).alias("prev3_duration_sec_decile"),
        F.rank().over(w_is_explicit_by_release_date_5).alias("release_date_year_cume_dist_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "release_date_year",
        "running_bpm",
        "next_release_date",
        "duration_sec_decile",
        "prev3_release_date",
        "prev3_release_date_2",
        "duration_sec_tercile",
        "second_prev3_release_date",
        "days_since_prev_next_release_date_2",
        "prev3_duration_sec_decile",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
    .orderBy(F.col("duration_sec_decile").asc(), F.col("release_date_year").asc())
)
