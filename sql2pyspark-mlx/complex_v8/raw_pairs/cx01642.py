from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_moving_1 = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_duration_sec_filled_f = Window.orderBy(F.col("duration_sec_filled").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_year_by_duratio = Window.partitionBy(F.year("release_date")).orderBy("duration_sec")
w_all_by_release_date_running = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_duration_sec_rank = Window.partitionBy("duration_sec_rank")
w_duration_sec_rank_by_duratio = Window.partitionBy("duration_sec_rank").orderBy("duration_sec_filled_vs_artist_id_avg")
w_release_date_month = Window.partitionBy(F.month("release_date"))

scoped = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

windowed = (
    scoped
    .select(
        "id",
        "duration_sec",
        "release_date",
        "duration_sec_filled",
        F.sum("duration_sec_filled").over(w_all_by_release_date_moving_1).alias("duration_sec_filled_sum_2"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.first("duration_sec_minus_bpm").over(w_all_by_duration_sec_filled_f).alias("first_duration_sec_minus_bpm"),
        F.last("duration_sec").over(w_all_by_duration_sec_filled_f).alias("last_duration_sec"),
        F.rank().over(w_release_date_year_by_duratio).alias("duration_sec_rank"),
        F.nth_value("duration_sec_filled", 2).over(w_all_by_duration_sec_filled_f).alias("second_duration_sec_filled"),
        F.round(F.col("duration_sec_minus_bpm") / F.nullif(F.sum("duration_sec_minus_bpm").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_minus_bpm_share_of_artist_id"),
        F.round(F.col("duration_sec_filled") - F.avg("duration_sec_filled").over(w_artist_id), 2).alias("duration_sec_filled_vs_artist_id_avg"),
        F.count(F.lit(1)).over(w_all_by_release_date_running).alias("running_count"),
    )
)

ranked = (
    windowed
    .select(
        "*",
        F.count(F.lit(1)).over(w_duration_sec_rank).alias("rows_in_duration_sec_rank"),
        F.round(F.percent_rank().over(w_duration_sec_rank_by_duratio), 3).alias("duration_sec_filled_vs_artist_id_avg_percent_ran"),
        F.max("duration_sec").over(w_release_date_month).alias("release_date_month_max_duration_sec"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_release_date_month), 2).alias("duration_sec_vs_release_date_month_avg"),
    )
)

result = (
    ranked
    .filter(F.col("duration_sec_vs_release_date_month_avg") <= 210.0)
    .select(
        "id",
        "release_date",
        "duration_sec_filled",
        "duration_sec_filled_sum_2",
        "rows_in_artist_id",
        "first_duration_sec_minus_bpm",
        "duration_sec_rank",
        "second_duration_sec_filled",
        "duration_sec_minus_bpm_share_of_artist_id",
        "duration_sec_filled_vs_artist_id_avg",
        "duration_sec_filled_vs_artist_id_avg_percent_ran",
        "release_date_month_max_duration_sec",
        F.year(F.col("release_date")).alias("release_date_year"),
    )
    .orderBy("duration_sec_filled_vs_artist_id_avg")
)
