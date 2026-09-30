from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_bpm = Window.partitionBy("is_explicit", "artist_id").orderBy("bpm")
w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date", "id").rowsBetween(-2, Window.currentRow)
w_is_explicit_artist_id_by_rel_2 = Window.partitionBy("is_explicit", "artist_id").orderBy("release_date", "id")
w_release_date_month_is_explic = Window.partitionBy(F.month("release_date"), "is_explicit")
w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("release_date").desc(), "id")
w_is_explicit_artist_id_by_rel_3 = Window.partitionBy("is_explicit", "artist_id").orderBy("release_date")
w_is_explicit_artist_id_by_bpm_2 = Window.partitionBy("is_explicit", "artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")

valid = df_tracks.filter(F.lower(F.col("title")).like("%ou%"))

ranked = (
    valid
    .select(
        "*",
        F.rank().over(w_is_explicit_artist_id_by_bpm).alias("bpm_rank"),
        F.nth_value("duration_sec", 2).over(w_is_explicit_artist_id_by_rel).alias("second_duration_sec"),
        F.round(F.avg("bpm").over(w_is_explicit_by_release_date), 2).alias("bpm_avg_3"),
        F.ntile(10).over(w_is_explicit_artist_id_by_rel_2).alias("release_date_decile"),
        F.lag("duration_sec", 2).over(w_is_explicit_artist_id_by_rel_2).alias("prev2_duration_sec"),
        F.sum("bpm").over(w_release_date_month_is_explic).alias("release_date_month_is_explicit_sum_bpm"),
        F.round(F.percent_rank().over(w_artist_id_is_explicit_by_rel), 3).alias("release_date_percent_rank"),
        F.round(F.cume_dist().over(w_is_explicit_artist_id_by_rel_3), 3).alias("release_date_cume_dist"),
        F.row_number().over(w_is_explicit_artist_id_by_rel_2).alias("rn"),
        F.first("duration_sec").over(w_is_explicit_artist_id_by_rel).alias("first_duration_sec"),
        F.sum("bpm").over(w_is_explicit_artist_id_by_bpm_2).alias("running_bpm"),
        F.sum("bpm").over(w_artist_id).alias("artist_id_sum_bpm"),
    )
)

scored = (
    ranked
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
        "bpm_rank",
        "second_duration_sec",
        "bpm_avg_3",
        "release_date_decile",
        "prev2_duration_sec",
        "release_date_month_is_explicit_sum_bpm",
        "release_date_percent_rank",
        "rn",
        "first_duration_sec",
        "running_bpm",
        "artist_id_sum_bpm",
        F.add_months(F.col("release_date"), 12).alias("release_date_plus_12m"),
    )
)

result = (
    scored
    .select(
        "id",
        "title",
        "duration_sec",
        "moods",
        "bpm_rank",
        "second_duration_sec",
        "bpm_avg_3",
        "release_date_month_is_explicit_sum_bpm",
        "release_date_percent_rank",
        "running_bpm",
        "release_date_plus_12m",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
    .orderBy("bpm_rank", F.col("release_date_percent_rank").desc())
)
