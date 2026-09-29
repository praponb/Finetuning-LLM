from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_all_by_bpm_full = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm = Window.orderBy(F.col("bpm").desc(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy(F.col("bpm").asc_nulls_last(), "id")
w_is_explicit_artist_id_by_bpm = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(-1, Window.currentRow)
w_is_explicit_artist_id_by_bpm_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").desc())
w_artist_id_by_release_date_ti = Window.partitionBy("artist_id").orderBy(F.col("release_date").asc_nulls_last())
w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.month("release_date")).orderBy("release_date", "id")
w_artist_id_by_release_date_ru = Window.partitionBy("artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_release_date_dense = Window.partitionBy("artist_id", "release_date_dense_rank")
w_all_by_bpm_full_2 = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_2 = Window.orderBy("bpm", "id")
w_artist_id_by_second_duration = Window.partitionBy("artist_id").orderBy(F.col("second_duration_sec").desc())

filtered = (
    df_tracks
    .filter((F.col("artist_id") >= 8) & (F.upper(F.trim(F.col("title"))).like("E%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.max("bpm").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_max_bpm"),
        F.nth_value("duration_sec", 2).over(w_all_by_bpm_full).alias("second_duration_sec"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_all_by_bpm)).alias("days_since_prev_release_date"),
        F.min("bpm").over(w_artist_id).alias("artist_id_min_bpm"),
        (F.col("bpm") - F.lag("bpm").over(w_is_explicit_by_bpm)).alias("bpm_change"),
        F.sum("bpm").over(w_artist_id).alias("artist_id_sum_bpm"),
        F.round(F.avg("bpm").over(w_is_explicit_artist_id_by_bpm), 2).alias("bpm_avg_2"),
        F.round(F.percent_rank().over(w_is_explicit_artist_id_by_bpm_2), 3).alias("bpm_percent_rank"),
        F.lead("release_date").over(w_all_by_bpm).alias("next_release_date"),
        F.round(F.percent_rank().over(w_artist_id_by_release_date_ti), 3).alias("release_date_percent_rank"),
        F.dense_rank().over(w_is_explicit_release_date_mon).alias("release_date_dense_rank"),
        F.lag("duration_sec", 1, 0).over(w_is_explicit_release_date_mon).alias("prev_duration_sec"),
        F.sum("bpm").over(w_artist_id_by_release_date_ru).alias("running_bpm"),
    )
)

scored = (
    metrics
    .filter(F.col("second_duration_sec") >= 142)
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        "release_date_label",
        "is_explicit_artist_id_max_bpm",
        "second_duration_sec",
        "artist_id_min_bpm",
        "artist_id_sum_bpm",
        "bpm_avg_2",
        "release_date_dense_rank",
        F.greatest(F.col("bpm_avg_2"), F.col("duration_sec")).alias("greatest_bpm_avg_2_duration_sec"),
        F.greatest(F.col("bpm_avg_2"), F.col("artist_id_sum_bpm")).alias("greatest_bpm_avg_2_artist_id_sum_bpm"),
        F.month(F.col("next_release_date")).alias("next_release_date_month"),
    )
)

with_trends = (
    scored
    .select(
        "id",
        "artist_id",
        "bpm",
        "moods",
        "release_date_label",
        "second_duration_sec",
        "artist_id_sum_bpm",
        "bpm_avg_2",
        "release_date_dense_rank",
        "greatest_bpm_avg_2_artist_id_sum_bpm",
        "next_release_date_month",
        F.sum("bpm_avg_2").over(w_artist_id_release_date_dense).alias("artist_id_release_date_dense_rank_sum_bpm_avg_2"),
        F.last("release_date_dense_rank").over(w_all_by_bpm_full_2).alias("last_release_date_dense_rank"),
        F.ntile(10).over(w_all_by_bpm_2).alias("bpm_decile"),
        F.round(F.avg("next_release_date_month").over(w_artist_id_release_date_dense), 2).alias("artist_id_release_date_dense_rank_avg_next_relea"),
        F.rank().over(w_artist_id_by_second_duration).alias("second_duration_sec_rank"),
        F.sum("release_date_dense_rank").over(w_artist_id_release_date_dense).alias("artist_id_release_date_dense_rank_sum_release_da"),
    )
)

grouped = (
    with_trends
    .filter(F.col("artist_id_release_date_dense_rank_sum_release_da").between(31, 60))
    .groupBy("next_release_date_month", "artist_id", "last_release_date_dense_rank")
    .agg(
        F.round(F.sum("last_release_date_dense_rank") / F.nullif(F.sum("second_duration_sec_rank"), F.lit(0)), 3).alias("last_release_date_dense_rank_to_second_duration_"),
        F.min("next_release_date_month").alias("min_next_release_date_month"),
        F.count("*").alias("n_rows"),
        F.round(F.stddev("artist_id_release_date_dense_rank_sum_bpm_avg_2"), 2).alias("stddev_artist_id_release_date_dense_rank_sum_bpm"),
        F.round(F.stddev("release_date_dense_rank"), 2).alias("stddev_release_date_dense_rank"),
        F.max("next_release_date_month").alias("max_next_release_date_month"),
    )
    .filter(F.col("n_rows") >= 3)
)

result = (
    grouped
    .select(
        "next_release_date_month",
        "artist_id",
        "min_next_release_date_month",
        "n_rows",
        "stddev_artist_id_release_date_dense_rank_sum_bpm",
        "stddev_release_date_dense_rank",
        "max_next_release_date_month",
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket"),
        F.abs(F.col("n_rows") - 5).alias("n_rows_distance"),
    )
    .orderBy(
        F.col("next_release_date_month").asc_nulls_last(),
        F.col("n_rows_bucket").desc_nulls_first(),
    )
)
