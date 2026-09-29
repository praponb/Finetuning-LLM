from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_by_bpm_fu = Window.partitionBy(F.month("release_date")).orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy("release_date", "id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").asc_nulls_last())
w_is_explicit_by_release_date_3 = Window.partitionBy("is_explicit").orderBy("release_date")
w_is_explicit_artist_id_by_rel_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_release_date_4 = Window.partitionBy("is_explicit").orderBy("release_date", "id").rowsBetween(-3, -1)
w_is_explicit_artist_id_by_rel_3 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_release_date_month_by_bpm_ti = Window.partitionBy(F.month("release_date")).orderBy(F.col("bpm").desc())

valid = (
    df_tracks
    .filter((F.col("bpm").between(73, 143)) | (F.col("release_date") >= F.lit("2024-07-01").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

ranked = (
    valid
    .select(
        "*",
        F.first("bpm_per_duration_sec").over(w_release_date_month_by_bpm_fu).alias("first_bpm_per_duration_sec"),
        F.nth_value("bpm_per_duration_sec", 2).over(w_is_explicit_by_release_date).alias("second_bpm_per_duration_sec"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_is_explicit_by_release_date_2)).alias("duration_sec_change"),
        F.round(F.cume_dist().over(w_is_explicit_artist_id_by_rel), 3).alias("release_date_cume_dist"),
        F.round(F.cume_dist().over(w_is_explicit_by_release_date_3), 3).alias("release_date_cume_dist_2"),
        F.nth_value("bpm", 2).over(w_is_explicit_artist_id_by_rel_2).alias("second_bpm"),
        F.last("release_date").over(w_is_explicit_artist_id_by_rel_2).alias("last_release_date"),
        F.sum("bpm_per_duration_sec").over(w_is_explicit_by_release_date_4).alias("bpm_per_duration_sec_sum_3_prior"),
        F.round(F.avg("duration_sec").over(w_is_explicit_artist_id_by_rel_3), 2).alias("duration_sec_avg_1_prior"),
        F.rank().over(w_release_date_month_by_bpm_ti).alias("bpm_rank"),
    )
)

grouped = (
    ranked
    .filter(F.col("artist_id") >= 3)
    .groupBy(
        F.year("release_date").alias("release_date_year"),
        F.date_format("last_release_date", "yyyy-MM").alias("last_release_date_ym"),
        F.when(F.col("bpm") >= 142, "large").when(F.col("bpm") >= 85, "medium").otherwise("small").alias("bpm_tier"),
    )
    .agg(
        F.round(F.avg("bpm"), 2).alias("avg_bpm"),
        F.count("*").alias("row_count"),
    )
)

result = (
    grouped
    .filter(F.col("last_release_date_ym").isNotNull())
    .select(
        "release_date_year",
        "bpm_tier",
        "avg_bpm",
        "row_count",
        (F.floor(F.col("row_count") / 2) * 2).alias("row_count_bucket"),
    )
    .orderBy("row_count_bucket")
)
