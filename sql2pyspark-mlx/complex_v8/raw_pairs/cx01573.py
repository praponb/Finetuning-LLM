from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").asc_nulls_last())
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-1, Window.currentRow)
w_artist_id_by_release_date_mo = Window.partitionBy("artist_id").orderBy("release_date", "id").rowsBetween(-6, Window.currentRow)
w_artist_id_by_release_date_ti = Window.partitionBy("artist_id").orderBy("release_date")
w_is_explicit_artist_id_by_dur_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_is_explicit = Window.partitionBy("is_explicit")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_release_date_month_by_durati = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy("duration_sec_avg_7_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_duration_sec_dense_rank_is_e = Window.partitionBy("duration_sec_dense_rank", "is_explicit")
w_artist_id_duration_sec_dense = Window.partitionBy("artist_id", "duration_sec_dense_rank")
w_release_date_month_by_durati_2 = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy("duration_sec_avg_7_bucket", "id")
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")

recent = (
    df_tracks
    .filter((F.col("duration_sec") < 312) | (F.col("release_date") >= F.lit("2024-08-26").cast("date")))
)

ranked = (
    recent
    .select(
        "*",
        F.dense_rank().over(w_is_explicit_artist_id_by_dur).alias("duration_sec_dense_rank"),
        F.max("bpm").over(w_is_explicit_by_duration_sec).alias("bpm_max_2"),
        F.round(F.avg("duration_sec").over(w_artist_id_by_release_date_mo), 2).alias("duration_sec_avg_7"),
        F.round(F.percent_rank().over(w_artist_id_by_release_date_ti), 3).alias("release_date_percent_rank"),
        F.round(F.avg("bpm").over(w_is_explicit_artist_id_by_dur_2), 2).alias("bpm_avg_2"),
        F.round(F.avg("bpm").over(w_is_explicit), 2).alias("is_explicit_avg_bpm"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit), F.lit(0)), 4).alias("bpm_share_of_is_explicit"),
        F.lag("release_date").over(w_is_explicit_by_duration_sec_2).alias("prev_release_date"),
    )
)

scored = (
    ranked
    .select(
        "*",
        F.abs(F.col("bpm") - 96).alias("bpm_distance"),
        (F.floor(F.col("duration_sec_avg_7") / 50) * 50).alias("duration_sec_avg_7_bucket"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.min("duration_sec").over(w_release_date_month_by_durati).alias("min_duration_sec_so_far"),
        F.round(F.col("duration_sec_avg_7_bucket") / F.nullif(F.sum("duration_sec_avg_7_bucket").over(w_duration_sec_dense_rank_is_e), F.lit(0)), 4).alias("duration_sec_avg_7_bucket_share_of_duration_sec_"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_duration_sec_dense), 2).alias("duration_sec_vs_artist_id_duration_sec_dense_ran"),
        (F.col("bpm_share_of_is_explicit") - F.lag("bpm_share_of_is_explicit").over(w_release_date_month_by_durati_2)).alias("bpm_share_of_is_explicit_change"),
        F.rank().over(w_all_by_duration_sec).alias("duration_sec_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "duration_sec_dense_rank",
        "bpm_max_2",
        "release_date_percent_rank",
        "is_explicit_avg_bpm",
        "bpm_share_of_is_explicit",
        "prev_release_date",
        "bpm_distance",
        "duration_sec_avg_7_bucket_share_of_duration_sec_",
        "duration_sec_vs_artist_id_duration_sec_dense_ran",
        "bpm_share_of_is_explicit_change",
        "duration_sec_rank",
        (F.floor(F.col("min_duration_sec_so_far") / 1000) * 1000).alias("min_duration_sec_so_far_bucket"),
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
    )
)
