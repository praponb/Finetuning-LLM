from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))
w_is_explicit_by_bpm_full = Window.partitionBy("is_explicit").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_by_bpm_ties = Window.partitionBy("is_explicit").orderBy("bpm")
w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.month("release_date")).orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy("release_date", "id")
w_is_explicit_release_date_mon_2 = Window.partitionBy("is_explicit", F.month("release_date")).orderBy("bpm", "id")
w_is_explicit_by_bpm_running = Window.partitionBy("is_explicit").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "sad")) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

windowed = (
    cleaned
    .select(
        "*",
        F.round(F.col("bpm") - F.avg("bpm").over(w_release_date_month), 2).alias("bpm_vs_release_date_month_avg"),
        F.nth_value("release_date", 2).over(w_is_explicit_by_bpm_full).alias("second_release_date"),
        F.round(F.avg("bpm").over(w_artist_id), 2).alias("artist_id_avg_bpm"),
        F.rank().over(w_is_explicit_by_bpm_ties).alias("bpm_rank"),
        F.first("bpm").over(w_is_explicit_release_date_mon).alias("first_bpm"),
        F.round(F.cume_dist().over(w_artist_id_is_explicit_by_rel), 3).alias("release_date_cume_dist"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_is_explicit_release_date_mon_2)).alias("days_since_prev_release_date"),
        F.min("bpm").over(w_artist_id).alias("artist_id_min_bpm"),
        F.last("bpm", ignorenulls=True).over(w_is_explicit_by_bpm_running).alias("last_known_bpm"),
    )
)

top_rows = (
    windowed
    .filter(F.col("bpm_rank") <= 3)
    .drop("bpm_rank")
)

result = (
    top_rows
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm_vs_release_date_month_avg",
        "second_release_date",
        "artist_id_avg_bpm",
        "first_bpm",
        "release_date_cume_dist",
        "artist_id_min_bpm",
        "last_known_bpm",
        F.date_format(F.col("second_release_date"), "MMM yyyy").alias("second_release_date_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("second_release_date")).alias("days_since_second_release_date"),
    )
    .orderBy(
        F.col("second_release_date_label").desc_nulls_first(),
        F.col("artist_id_min_bpm").asc(),
        "id",
    )
    .limit(5)
)
