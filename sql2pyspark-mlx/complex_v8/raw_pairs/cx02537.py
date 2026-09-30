from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_release_date_fu = Window.partitionBy("artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm_ties = Window.partitionBy("artist_id").orderBy("bpm")
w_all_by_release_date_prior_2 = Window.orderBy("release_date", "id").rowsBetween(-2, -1)
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id")
w_all_by_duration_sec_running = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_all_by_bpm_share_of_artist_i = Window.orderBy(F.col("bpm_share_of_artist_id").desc(), "id")
w_all_by_bpm_max_2_prior_fille = Window.orderBy(F.col("bpm_max_2_prior_filled").desc(), "id")
w_all_by_artist_id_min_duratio = Window.orderBy("artist_id_min_duration_sec", "id")

base = (
    df_tracks
    .filter(F.col("duration_sec") >= 144)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

metrics = (
    base
    .select(
        "id",
        "bpm",
        "moods",
        F.lag("bpm_bucket").over(w_artist_id_by_bpm).alias("prev_bpm_bucket"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.nth_value("bpm_bucket", 2).over(w_artist_id_by_release_date_fu).alias("second_bpm_bucket"),
        F.max("bpm_bucket").over(w_artist_id).alias("artist_id_max_bpm_bucket"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_id_by_bpm)).alias("duration_sec_change"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.round(F.cume_dist().over(w_artist_id_by_bpm_ties), 3).alias("bpm_cume_dist"),
        F.max("bpm").over(w_all_by_release_date_prior_2).alias("bpm_max_2_prior"),
        F.min("duration_sec").over(w_artist_id).alias("artist_id_min_duration_sec"),
        F.row_number().over(w_artist_id_by_release_date).alias("rn"),
        F.last("bpm", ignorenulls=True).over(w_all_by_duration_sec_running).alias("last_known_bpm"),
        F.lag("bpm_bucket").over(w_all_by_duration_sec).alias("prev_bpm_bucket_2"),
    )
)

enriched = (
    metrics
    .filter(F.col("bpm_max_2_prior") < 420)
    .select(
        "id",
        "bpm",
        "moods",
        "prev_bpm_bucket",
        "second_bpm_bucket",
        "artist_id_max_bpm_bucket",
        "duration_sec_change",
        "bpm_share_of_artist_id",
        "bpm_cume_dist",
        "artist_id_min_duration_sec",
        F.coalesce(F.col("bpm_max_2_prior"), F.lit(0)).alias("bpm_max_2_prior_filled"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_bpm_share_of_artist_i), 3).alias("bpm_share_of_artist_id_percent_rank"),
        F.dense_rank().over(w_all_by_bpm_max_2_prior_fille).alias("bpm_max_2_prior_filled_dense_rank"),
        (F.col("bpm_cume_dist") - F.lag("bpm_cume_dist").over(w_all_by_artist_id_min_duratio)).alias("bpm_cume_dist_change"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "bpm",
        "moods",
        "prev_bpm_bucket",
        "second_bpm_bucket",
        "artist_id_max_bpm_bucket",
        "duration_sec_change",
        "artist_id_min_duration_sec",
        "bpm_max_2_prior_filled",
        "bpm_share_of_artist_id_percent_rank",
        "bpm_max_2_prior_filled_dense_rank",
        "bpm_cume_dist_change",
        F.when(F.col("bpm") >= 142, "A").when(F.col("bpm") >= 100, "B").otherwise("C").alias("bpm_tier"),
    )
    .orderBy(F.col("bpm_tier").desc_nulls_first())
)
