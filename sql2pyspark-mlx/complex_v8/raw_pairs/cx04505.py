from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_all_by_bpm_ties = Window.orderBy(F.col("bpm").desc())
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_all_by_bpm_full = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_duration_sec_pr = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-3, -1)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_bpm_full_2 = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id")

valid_tracks = (
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
    )
)

with_trends = (
    valid_tracks
    .select(
        "*",
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.dense_rank().over(w_all_by_bpm_ties).alias("bpm_dense_rank"),
        F.row_number().over(w_all_by_duration_sec).alias("rn"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_is_explicit), 2).alias("bpm_vs_is_explicit_avg"),
        F.first("duration_sec").over(w_all_by_bpm_full).alias("first_duration_sec"),
        F.sum("bpm").over(w_artist_id_by_duration_sec_pr).alias("bpm_sum_3_prior"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
        F.lag("duration_sec", 3, 0).over(w_all_by_duration_sec).alias("prev3_duration_sec"),
        F.first("bpm").over(w_all_by_bpm_full_2).alias("first_bpm"),
        F.sum("duration_sec").over(w_is_explicit).alias("is_explicit_sum_duration_sec"),
        F.row_number().over(w_is_explicit_artist_id_by_dur).alias("duration_sec_seq"),
    )
)

labeled = (
    with_trends
    .select(
        "*",
        F.coalesce(F.col("first_duration_sec"), F.lit(0)).alias("first_duration_sec_filled"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.round(F.col("rows_in_is_explicit") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("rows_in_is_explicit_per_duration_sec"),
    )
)

result = (
    labeled
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        "rows_in_is_explicit",
        "bpm_dense_rank",
        "first_duration_sec",
        "bpm_sum_3_prior",
        "duration_sec_share_of_artist_id",
        "prev3_duration_sec",
        "first_bpm",
        "is_explicit_sum_duration_sec",
        "duration_sec_seq",
        "first_duration_sec_filled",
        "is_explicit_flag",
        "rows_in_is_explicit_per_duration_sec",
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_ref"),
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
    )
    .orderBy(F.col("first_bpm").desc_nulls_first())
)
