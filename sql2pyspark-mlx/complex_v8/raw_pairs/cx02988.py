from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date")).orderBy(F.col("bpm").desc(), "id")
w_artist_id_release_date_year = Window.partitionBy("artist_id", F.year("release_date")).orderBy("bpm")
w_all_by_release_date = Window.orderBy("release_date", "id")
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_artist_id_by_release_date_mo = Window.partitionBy("artist_id").orderBy("release_date", "id").rowsBetween(-6, Window.currentRow)
w_artist_id_release_date_month_2 = Window.partitionBy("artist_id", F.date_trunc("month", "release_date")).orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm_tercile = Window.partitionBy("artist_id").orderBy(F.col("bpm_tercile").desc(), "id")
w_bpm_decile_bpm_tercile = Window.partitionBy("bpm_decile", "bpm_tercile")
w_all_by_last_release_date_tie = Window.orderBy("last_release_date")
w_bpm_quartile_bpm_tercile = Window.partitionBy("bpm_quartile", "bpm_tercile")
w_all_by_last_release_date_run = Window.orderBy("last_release_date", "bpm_quartile", "bpm_tercile").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tracks
    .filter(F.col("artist_id") >= 8)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.ntile(10).over(w_artist_id_release_date_month).alias("bpm_decile"),
        F.round(F.percent_rank().over(w_artist_id_release_date_year), 3).alias("bpm_percent_rank"),
        F.ntile(3).over(w_artist_id_release_date_month).alias("bpm_tercile"),
        F.dense_rank().over(w_all_by_release_date).alias("release_date_dense_rank"),
        F.ntile(4).over(w_artist_id_release_date_month).alias("bpm_quartile"),
        F.sum("bpm").over(w_all_by_duration_sec_running).alias("running_bpm"),
        F.round(F.percent_rank().over(w_all_by_duration_sec), 3).alias("duration_sec_percent_rank"),
        F.round(F.avg("bpm").over(w_artist_id), 2).alias("artist_id_avg_bpm"),
        F.round(F.avg("bpm").over(w_artist_id_by_release_date_mo), 2).alias("bpm_avg_7"),
        F.nth_value("bpm", 2).over(w_artist_id_release_date_month_2).alias("second_bpm"),
        F.nth_value("release_date", 2).over(w_artist_id_release_date_month_2).alias("second_release_date"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "moods",
        "bpm_decile",
        "bpm_percent_rank",
        "bpm_tercile",
        "bpm_quartile",
        "running_bpm",
        "artist_id_avg_bpm",
        "bpm_avg_7",
        "second_bpm",
        "second_release_date",
        F.dense_rank().over(w_artist_id_by_bpm_tercile).alias("bpm_tercile_dense_rank"),
        F.size(F.collect_set("artist_id").over(w_bpm_decile_bpm_tercile)).alias("distinct_artist_id_in_bpm_decile_bpm_tercile"),
        F.round(F.col("duration_sec_percent_rank") / F.nullif(F.sum("duration_sec_percent_rank").over(w_bpm_decile_bpm_tercile), F.lit(0)), 4).alias("duration_sec_percent_rank_share_of_bpm_decile_bp"),
        F.lead("bpm_avg_7", 1, 0).over(w_artist_id_by_bpm_tercile).alias("next_bpm_avg_7"),
    )
)

summary = (
    with_trends_2
    .filter(F.col("second_release_date") < F.lit("2025-03-29").cast("date"))
    .groupBy("bpm_quartile", "bpm_tercile")
    .agg(
        F.max("release_date").alias("last_release_date"),
        F.min("release_date").alias("first_release_date"),
        F.max("bpm_percent_rank").alias("max_bpm_percent_rank_check"),
    )
    .filter(F.col("max_bpm_percent_rank_check") > 0.3)
    .drop("max_bpm_percent_rank_check")
)

with_windows = (
    summary
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_last_release_date_tie), 3).alias("last_release_date_cume_dist"),
        F.sum("bpm_tercile").over(w_all_by_last_release_date_run).alias("running_bpm_tercile"),
        F.count(F.lit(1)).over(w_all_by_last_release_date_run).alias("running_count"),
        F.round(F.col("bpm_tercile") / F.nullif(F.sum("bpm_tercile").over(w_bpm_quartile_bpm_tercile), F.lit(0)), 4).alias("bpm_tercile_share_of_bpm_quartile_bpm_tercile"),
    )
)

result = (
    with_windows
    .filter(F.col("last_release_date").between(F.lit("2024-05-12").cast("date"), F.lit("2025-11-14").cast("date")))
    .select(
        "bpm_quartile",
        "bpm_tercile",
        "last_release_date",
        "first_release_date",
        "last_release_date_cume_dist",
        "running_bpm_tercile",
        "bpm_tercile_share_of_bpm_quartile_bpm_tercile",
    )
    .orderBy(F.col("running_bpm_tercile").desc(), F.col("last_release_date").asc())
)
