from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec_filled").desc(), "id")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-3, -1)
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id")
w_artist_id_by_duration_sec_fi = Window.partitionBy("artist_id").orderBy("duration_sec_filled")
w_is_explicit_by_duration_sec_3 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-2, Window.currentRow)
w_artist_id_by_duration_sec_fi_2 = Window.partitionBy("artist_id").orderBy("duration_sec_filled", "id")
w_all_by_duration_sec_full = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_bpm_running = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_duration_sec_filled_seq_bpm = Window.partitionBy("duration_sec_filled_seq", "bpm_rank")
w_bpm_rank_n_rows_by_stddev_du = Window.partitionBy("bpm_rank", "n_rows").orderBy("stddev_duration_sec_sum_3", "bpm_rank", "duration_sec_filled_seq")

recent = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "party"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

with_trends = (
    recent
    .select(
        "id",
        "artist_id",
        "moods",
        "duration_sec_filled",
        F.lag("duration_sec", 2).over(w_is_explicit_by_duration_sec).alias("prev2_duration_sec"),
        F.row_number().over(w_artist_id_is_explicit_by_dur).alias("duration_sec_filled_seq"),
        F.max("duration_sec").over(w_is_explicit_by_duration_sec_2).alias("duration_sec_max_3_prior"),
        F.sum("duration_sec").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_sum_duration_sec"),
        F.rank().over(w_is_explicit_by_bpm).alias("bpm_rank"),
        F.round(F.cume_dist().over(w_artist_id_by_duration_sec_fi), 3).alias("duration_sec_filled_cume_dist"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit_artist_id), F.lit(0)), 4).alias("bpm_share_of_is_explicit_artist_id"),
        F.sum("duration_sec").over(w_is_explicit_by_duration_sec_3).alias("duration_sec_sum_3"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_by_duration_sec_fi_2)).alias("bpm_change"),
        F.round(F.col("duration_sec_filled") - F.avg("duration_sec_filled").over(w_is_explicit_artist_id), 2).alias("duration_sec_filled_vs_is_explicit_artist_id_avg"),
        F.last("duration_sec").over(w_all_by_duration_sec_full).alias("last_duration_sec"),
        F.first("duration_sec_filled", ignorenulls=True).over(w_is_explicit_by_bpm_running).alias("first_known_duration_sec_filled"),
        F.min("duration_sec").over(w_artist_id).alias("artist_id_min_duration_sec"),
    )
)

enriched = (
    with_trends
    .select(
        "*",
        (F.floor(F.col("bpm_change") / 50) * 50).alias("bpm_change_bucket"),
        (F.col("duration_sec_filled") - F.col("bpm_rank")).alias("duration_sec_filled_minus_bpm_rank"),
    )
)

stats = (
    enriched
    .groupBy("bpm_rank", "duration_sec_filled_seq")
    .agg(
        F.round(F.stddev("duration_sec_sum_3"), 2).alias("stddev_duration_sec_sum_3"),
        F.count("*").alias("n_rows"),
    )
)

windowed = (
    stats
    .select(
        "bpm_rank",
        "duration_sec_filled_seq",
        "stddev_duration_sec_sum_3",
        "n_rows",
        F.round(F.avg("bpm_rank").over(w_duration_sec_filled_seq_bpm), 2).alias("duration_sec_filled_seq_bpm_rank_avg_bpm_rank"),
        F.round(F.avg("stddev_duration_sec_sum_3").over(w_duration_sec_filled_seq_bpm), 2).alias("duration_sec_filled_seq_bpm_rank_avg_stddev_dura"),
        F.round(F.cume_dist().over(w_bpm_rank_n_rows_by_stddev_du), 3).alias("stddev_duration_sec_sum_3_cume_dist"),
    )
)

result = (
    windowed
    .filter(F.col("stddev_duration_sec_sum_3") >= 252.0)
    .select(
        "bpm_rank",
        "duration_sec_filled_seq",
        "stddev_duration_sec_sum_3",
        "n_rows",
        "duration_sec_filled_seq_bpm_rank_avg_bpm_rank",
        "stddev_duration_sec_sum_3_cume_dist",
        (F.floor(F.col("duration_sec_filled_seq") / 2) * 2).alias("duration_sec_filled_seq_bucket"),
        F.least(F.col("n_rows"), F.col("duration_sec_filled_seq")).alias("least_n_rows_duration_sec_filled_seq"),
    )
    .orderBy(
        "duration_sec_filled_seq_bpm_rank_avg_bpm_rank",
        F.col("bpm_rank").asc_nulls_last(),
    )
)
