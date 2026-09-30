from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_full = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_by_releas = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy("release_date", "id")
w_all_by_release_date_running = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_duration_sec_tier = Window.partitionBy("duration_sec_tier")
w_artist_id_by_bpm_ties = Window.partitionBy("artist_id").orderBy("bpm")
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_artist_id_by_bpm_full = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date_ties = Window.orderBy("release_date")
w_artist_id_seq_no_by_release = Window.partitionBy("artist_id", "seq_no").orderBy(F.col("release_date_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_known_release_date_mont = Window.partitionBy(F.date_trunc("month", "last_known_release_date"))
w_all_by_bpm = Window.orderBy("bpm", "id")
w_n_rows_by_total_bpm_change_f = Window.partitionBy("n_rows").orderBy(F.col("total_bpm_change").asc_nulls_last(), "artist_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows_by_total_bpm_change_m = Window.partitionBy("n_rows").orderBy(F.col("total_bpm_change").asc_nulls_last(), "artist_id").rowsBetween(-1, Window.currentRow)
w_artist_id_n_rows = Window.partitionBy("artist_id", "n_rows")

cleaned_tracks = (
    df_tracks
    .filter(F.col("artist_id") >= 10)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.when(F.col("duration_sec") >= 352, "large").when(F.col("duration_sec") >= 183, "medium").otherwise("small").alias("duration_sec_tier"),
    )
)

windowed = (
    cleaned_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
        "release_date_label",
        "duration_sec_tier",
        F.nth_value("release_date", 2).over(w_all_by_release_date_full).alias("second_release_date"),
        F.row_number().over(w_release_date_month_by_releas).alias("seq_no"),
        F.last("release_date", ignorenulls=True).over(w_all_by_release_date_running).alias("last_known_release_date"),
        F.size(F.collect_set("artist_id").over(w_duration_sec_tier)).alias("distinct_artist_id_in_duration_sec_tier"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_duration_sec_tier), F.lit(0)), 4).alias("bpm_share_of_duration_sec_tier"),
        F.round(F.percent_rank().over(w_release_date_month_by_releas), 3).alias("release_date_percent_rank"),
        F.dense_rank().over(w_artist_id_by_bpm_ties).alias("bpm_dense_rank"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_by_bpm)).alias("bpm_change"),
        F.nth_value("bpm", 2).over(w_artist_id_by_bpm_full).alias("second_bpm"),
        F.round(F.cume_dist().over(w_all_by_release_date_ties), 3).alias("release_date_cume_dist"),
    )
)

leaders = (
    windowed
    .filter((F.col("bpm_dense_rank") == 1) & (F.col("release_date_percent_rank") >= 0.2))
)

windowed_2 = (
    leaders
    .select(
        "*",
        F.first("bpm_change", ignorenulls=True).over(w_artist_id_seq_no_by_release).alias("first_known_bpm_change"),
        F.min("seq_no").over(w_last_known_release_date_mont).alias("last_known_release_date_month_min_seq_no"),
        (F.col("bpm") - F.lag("bpm").over(w_all_by_bpm)).alias("bpm_change_2"),
    )
)

per_group = (
    windowed_2
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("bpm_change").alias("total_bpm_change"),
    )
)

with_trends = (
    per_group
    .select(
        "*",
        F.last("total_bpm_change").over(w_n_rows_by_total_bpm_change_f).alias("last_total_bpm_change"),
        F.round(F.avg("n_rows").over(w_n_rows_by_total_bpm_change_m), 2).alias("n_rows_avg_2"),
        F.round(F.col("total_bpm_change") - F.avg("total_bpm_change").over(w_artist_id_n_rows), 2).alias("total_bpm_change_vs_artist_id_n_rows_avg"),
    )
)

result = (
    with_trends
    .filter(F.col("n_rows_avg_2") >= 4.8)
    .select(
        "artist_id",
        "n_rows",
        "total_bpm_change",
        "last_total_bpm_change",
        "n_rows_avg_2",
        "total_bpm_change_vs_artist_id_n_rows_avg",
        F.coalesce(F.col("n_rows"), F.lit(0)).alias("n_rows_filled"),
        (F.floor(F.col("total_bpm_change") / 250) * 250).alias("total_bpm_change_bucket"),
    )
    .orderBy(F.col("n_rows_filled").desc(), "artist_id")
    .limit(20)
)
