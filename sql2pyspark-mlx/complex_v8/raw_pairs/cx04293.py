from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_is_explic = Window.partitionBy(F.month("release_date"), "is_explicit").orderBy("release_date", "id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit = Window.partitionBy("is_explicit")
w_all_by_duration_sec_full = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_is_explic_2 = Window.partitionBy(F.month("release_date"), "is_explicit").orderBy("release_date", "id").rowsBetween(-3, Window.currentRow)
w_artist_id_is_explicit_by_rel_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_artist_id_is_explicit_by_bpm_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_release_date_ti = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc_nulls_first())
w_duration_sec_quartile_second = Window.partitionBy("duration_sec_quartile", F.month("second_release_date"))
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc_nulls_first(), "id")
w_duration_sec_quartile_by_bpm = Window.partitionBy("duration_sec_quartile").orderBy(F.col("bpm_bucket").desc())
w_duration_sec_quartile_by_bpm_2 = Window.partitionBy("duration_sec_quartile").orderBy(F.col("bpm_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_tracks
    .filter(F.col("release_date") < F.lit("2024-12-22").cast("date"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

windowed = (
    base
    .select(
        "*",
        F.nth_value("duration_sec", 2).over(w_artist_id_is_explicit_by_bpm).alias("second_duration_sec"),
        F.lag("bpm").over(w_release_date_month_is_explic).alias("prev_bpm"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_is_explicit_artist_id), 2).alias("bpm_vs_is_explicit_artist_id_avg"),
        F.sum("duration_sec").over(w_artist_id_is_explicit_by_rel).alias("running_duration_sec"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_is_explicit), 2).alias("duration_sec_vs_is_explicit_avg"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_is_explicit), F.lit(0)), 4).alias("duration_sec_share_of_is_explicit"),
        F.nth_value("release_date", 2).over(w_all_by_duration_sec_full).alias("second_release_date"),
        F.max("duration_sec").over(w_release_date_month_is_explic_2).alias("duration_sec_max_4"),
        F.nth_value("bpm", 2).over(w_artist_id_is_explicit_by_rel_2).alias("second_bpm"),
        F.sum("duration_sec").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_sum_duration_sec"),
        F.ntile(4).over(w_all_by_duration_sec).alias("duration_sec_quartile"),
        F.first("bpm", ignorenulls=True).over(w_artist_id_is_explicit_by_bpm_2).alias("first_known_bpm"),
    )
)

scored = (
    windowed
    .filter(F.col("second_duration_sec").between(165, 365))
    .select(
        "*",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        F.round(F.col("duration_sec") / F.nullif(F.col("duration_sec_max_4"), F.lit(0)), 3).alias("duration_sec_per_duration_sec_max_4"),
        (F.floor(F.col("first_known_bpm") / 25) * 25).alias("first_known_bpm_bucket"),
    )
)

ranked = (
    scored
    .select(
        "*",
        F.round(F.cume_dist().over(w_artist_id_by_release_date_ti), 3).alias("release_date_cume_dist"),
        F.round(F.col("second_duration_sec") / F.nullif(F.sum("second_duration_sec").over(w_duration_sec_quartile_second), F.lit(0)), 4).alias("second_duration_sec_share_of_duration_sec_quarti"),
        F.dense_rank().over(w_artist_id_by_release_date).alias("release_date_dense_rank"),
        F.round(F.percent_rank().over(w_duration_sec_quartile_by_bpm), 3).alias("bpm_bucket_percent_rank"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_duration_sec_quartile_second), F.lit(0)), 4).alias("bpm_share_of_duration_sec_quartile_second_releas"),
        F.nth_value("second_duration_sec", 2).over(w_duration_sec_quartile_by_bpm_2).alias("second_second_duration_sec"),
    )
)

result = (
    ranked
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "second_duration_sec",
        "prev_bpm",
        "bpm_vs_is_explicit_artist_id_avg",
        "duration_sec_vs_is_explicit_avg",
        "duration_sec_share_of_is_explicit",
        "second_release_date",
        "duration_sec_max_4",
        "second_bpm",
        "is_explicit_artist_id_sum_duration_sec",
        "duration_sec_quartile",
        "release_date_cume_dist",
        "second_duration_sec_share_of_duration_sec_quarti",
        "bpm_bucket_percent_rank",
        "bpm_share_of_duration_sec_quartile_second_releas",
        "second_second_duration_sec",
    )
    .orderBy(F.col("duration_sec_max_4").asc_nulls_last())
)
