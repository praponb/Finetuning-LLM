from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_artist_id_by_duration_sec_fu = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_artist_id_by_duration_sec_ti = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc_nulls_first())
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_is_explicit_artist_id_by_bpm = Window.partitionBy("is_explicit", "artist_id").orderBy("bpm_bucket", "id")
w_is_explicit_bpm_bucket_dense = Window.partitionBy("is_explicit", "bpm_bucket_dense_rank").orderBy(F.col("is_explicit_artist_id_min_duration_sec").asc_nulls_last(), "id")
w_duration_sec_dense_rank_by_a = Window.partitionBy("duration_sec_dense_rank").orderBy(F.col("artist_id_avg_bpm_bucket").desc(), "id").rowsBetween(-2, -1)
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")

valid = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-03-08").cast("date")) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        F.greatest(F.col("duration_sec"), F.col("bpm")).alias("greatest_duration_sec_bpm"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.round(F.col("greatest_duration_sec_bpm") / F.nullif(F.sum("greatest_duration_sec_bpm").over(w_artist_id), F.lit(0)), 4).alias("greatest_duration_sec_bpm_share_of_artist_id"),
        F.round(F.avg("bpm_bucket").over(w_artist_id), 2).alias("artist_id_avg_bpm_bucket"),
        F.dense_rank().over(w_is_explicit_by_duration_sec).alias("duration_sec_dense_rank"),
        F.nth_value("bpm_bucket", 2).over(w_artist_id_by_duration_sec_fu).alias("second_bpm_bucket"),
        F.rank().over(w_all_by_duration_sec).alias("duration_sec_rank"),
        F.min("duration_sec").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_min_duration_sec"),
        F.rank().over(w_artist_id_by_duration_sec_ti).alias("duration_sec_rank_2"),
        F.lead("duration_sec", 1, 0).over(w_all_by_duration_sec).alias("next_duration_sec"),
        F.max("bpm_bucket").over(w_artist_id).alias("artist_id_max_bpm_bucket"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_all_by_duration_sec)).alias("duration_sec_change"),
        F.lag("duration_sec").over(w_artist_id_by_duration_sec).alias("prev_duration_sec"),
        F.dense_rank().over(w_is_explicit_artist_id_by_bpm).alias("bpm_bucket_dense_rank"),
    )
)

ranked = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w_is_explicit_bpm_bucket_dense).alias("is_explicit_artist_id_min_duration_sec_dense_ran"),
        F.max("duration_sec_dense_rank").over(w_duration_sec_dense_rank_by_a).alias("duration_sec_dense_rank_max_2_prior"),
        F.max("duration_sec").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_max_duration_sec"),
    )
)

result = (
    ranked
    .select(
        "id",
        "title",
        "greatest_duration_sec_bpm_share_of_artist_id",
        "artist_id_avg_bpm_bucket",
        "duration_sec_dense_rank",
        "second_bpm_bucket",
        "duration_sec_rank_2",
        "artist_id_max_bpm_bucket",
        "prev_duration_sec",
        "is_explicit_artist_id_min_duration_sec_dense_ran",
        "duration_sec_dense_rank_max_2_prior",
        "artist_id_is_explicit_max_duration_sec",
    )
    .orderBy(F.col("artist_id_is_explicit_max_duration_sec").asc_nulls_last(), "id")
)
