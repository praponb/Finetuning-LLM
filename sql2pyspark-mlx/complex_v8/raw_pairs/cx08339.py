from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_artist_id_by_duration_sec_ti = Window.partitionBy("artist_id").orderBy("duration_sec")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_duration_sec_moving_3 = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy("duration_sec", "id")
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec_vs_artist_id_is_explicit_avg").desc_nulls_first(), "id")
w_artist_id_by_duration_sec_av = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_avg_4").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_artist_id_sum_duratio = Window.orderBy("artist_id_sum_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_is_explicit_by_dur_2 = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec_vs_artist_id_is_explicit_avg").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_duration_sec_av_2 = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_avg_4").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_tracks
    .filter((F.year(F.col("release_date")) == 2024) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id_is_explicit), 2).alias("bpm_vs_artist_id_is_explicit_avg"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_is_explicit), 2).alias("duration_sec_vs_artist_id_is_explicit_avg"),
        F.dense_rank().over(w_artist_id_by_duration_sec_ti).alias("duration_sec_dense_rank"),
        F.sum("duration_sec").over(w_artist_id).alias("artist_id_sum_duration_sec"),
        F.round(F.avg("duration_sec").over(w_all_by_duration_sec_moving_3), 2).alias("duration_sec_avg_4"),
        F.row_number().over(w_all_by_duration_sec).alias("seq_no"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.round(F.percent_rank().over(w_all_by_duration_sec), 3).alias("duration_sec_percent_rank"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_artist_id_by_duration_sec)).alias("days_since_prev_release_date"),
    )
)

first_rows = (
    with_windows
    .filter((F.col("duration_sec_dense_rank") <= 5) & (F.col("duration_sec_percent_rank") < 0.7))
)

sequenced = (
    first_rows
    .select(
        "*",
        F.row_number().over(w_artist_id_is_explicit_by_dur).alias("rn"),
        F.round(F.avg("days_since_prev_release_date").over(w_artist_id_by_duration_sec_av), 2).alias("days_since_prev_release_date_avg_2"),
        F.max("duration_sec_avg_4").over(w_all_by_artist_id_sum_duratio).alias("max_duration_sec_avg_4_so_far"),
        F.round(F.avg("duration_sec_percent_rank").over(w_is_explicit), 2).alias("is_explicit_avg_duration_sec_percent_rank"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_is_explicit), 2).alias("duration_sec_vs_is_explicit_avg"),
        F.first("bpm").over(w_artist_id_is_explicit_by_dur_2).alias("first_bpm"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.last("rows_in_artist_id").over(w_artist_id_by_duration_sec_av_2).alias("last_rows_in_artist_id"),
    )
)

per_group = (
    sequenced
    .groupBy(
        "seq_no",
        "is_explicit",
        F.when(F.col("bpm") >= 165, "large").when(F.col("bpm") >= 100, "medium").otherwise("small").alias("bpm_tier"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("rn").alias("min_rn"),
    )
)

result = (
    per_group
    .filter(F.col("bpm_tier").isin("medium", "large"))
    .select(
        "seq_no",
        "is_explicit",
        "stddev_duration_sec",
        "distinct_ids",
        "min_rn",
        F.coalesce(F.col("seq_no"), F.lit(0)).alias("seq_no_filled"),
        F.coalesce(F.col("seq_no"), F.lit(0)).alias("seq_no_filled_2"),
    )
)
