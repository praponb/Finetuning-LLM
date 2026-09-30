from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_release_date_month_by_bpm_ru = Window.partitionBy(F.month("release_date")).orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_days_since_release_da = Window.orderBy("days_since_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_all_by_days_since_release_da_2 = Window.orderBy("days_since_release_date", "id")
w_last_known_release_date_year = Window.partitionBy(F.year("last_known_release_date")).orderBy("bpm", "id")
w_all_by_days_since_release_da_3 = Window.orderBy(F.col("days_since_release_date").desc(), "id").rowsBetween(-6, Window.currentRow)
w_release_date_year = Window.partitionBy(F.year("release_date"))

recent = (
    df_tracks
    .filter(F.col("duration_sec") <= 352)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_ref"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.sum("days_since_release_date").over(w_all_by_duration_sec_running).alias("running_days_since_release_date"),
        F.last("release_date", ignorenulls=True).over(w_all_by_duration_sec_running).alias("last_known_release_date"),
        F.round(F.avg("duration_sec").over(w_artist_id), 2).alias("artist_id_avg_duration_sec"),
        F.count(F.lit(1)).over(w_release_date_month_by_bpm_ru).alias("running_count"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.last("duration_sec").over(w_all_by_days_since_release_da).alias("last_duration_sec"),
        F.round(F.percent_rank().over(w_all_by_duration_sec), 3).alias("duration_sec_percent_rank"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        F.first("duration_sec", ignorenulls=True).over(w_all_by_duration_sec_running).alias("first_known_duration_sec"),
        F.ntile(4).over(w_all_by_days_since_release_da_2).alias("days_since_release_date_quartile"),
        F.round(F.avg("bpm").over(w_artist_id), 2).alias("artist_id_avg_bpm"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        (F.col("days_since_release_date") - F.lag("days_since_release_date").over(w_all_by_days_since_release_da_2)).alias("days_since_release_date_change"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.ntile(3).over(w_last_known_release_date_year).alias("bpm_tercile"),
        F.max("days_since_release_date").over(w_all_by_days_since_release_da_3).alias("days_since_release_date_max_7"),
        F.lead("running_count").over(w_last_known_release_date_year).alias("next_running_count"),
        F.round(F.col("days_since_release_date") - F.avg("days_since_release_date").over(w_release_date_year), 2).alias("days_since_release_date_vs_release_date_year_avg"),
    )
)

result = (
    windowed
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "days_since_release_date",
        "title_ref",
        "running_days_since_release_date",
        "last_known_release_date",
        "artist_id_avg_duration_sec",
        "running_count",
        "rows_in_artist_id",
        "last_duration_sec",
        "duration_sec_percent_rank",
        "first_known_duration_sec",
        "days_since_release_date_quartile",
        "bpm_vs_artist_id_avg",
        "days_since_release_date_change",
        "bpm_tercile",
        "days_since_release_date_max_7",
        "next_running_count",
        "days_since_release_date_vs_release_date_year_avg",
        F.round(F.col("next_running_count") / F.nullif(F.col("rows_in_artist_id"), F.lit(0)), 3).alias("next_running_count_per_rows_in_artist_id"),
    )
    .orderBy("days_since_release_date_max_7", F.col("release_date").desc(), "id")
    .limit(10)
)
