from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_ym_len_by_media = Window.partitionBy("release_date_ym_len").orderBy(F.col("median_bpm_bucket_filled").desc_nulls_first(), "release_date_ym")
w_release_date_ym_len = Window.partitionBy("release_date_ym_len")
w_release_date_ym_len_by_durat = Window.partitionBy("release_date_ym_len").orderBy("duration_sec_is_explicit_true", "release_date_ym")

cleaned_tracks = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.col("bpm") <= 142))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

enriched = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre") == "Classical")
    .select("s.*", F.col("a.genre").alias("artist_genre"))
)

grouped = (
    enriched
    .groupBy(F.date_format("release_date", "yyyy-MM").alias("release_date_ym"))
    .agg(
        F.sum(F.when(F.col("is_explicit"), F.col("duration_sec"))).alias("duration_sec_is_explicit_true"),
        F.sum(F.when(F.col("artist_genre") == "Jazz", F.col("bpm_bucket")).otherwise(0)).alias("bpm_bucket_artist_genre_jazz"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.percentile_approx("bpm_bucket", 0.5).alias("median_bpm_bucket"),
        F.sum("duration_sec").alias("sum_duration_sec_check"),
    )
    .filter(F.col("sum_duration_sec_check") > 164)
    .drop("sum_duration_sec_check")
)

derived = (
    grouped
    .select(
        "release_date_ym",
        "duration_sec_is_explicit_true",
        "bpm_bucket_artist_genre_jazz",
        "median_bpm_bucket",
        F.coalesce(F.col("median_bpm_bucket"), F.lit(0)).alias("median_bpm_bucket_filled"),
        F.length(F.trim(F.col("release_date_ym"))).alias("release_date_ym_len"),
    )
)

with_activity = (
    derived
    .crossJoin(derived.agg(F.avg("bpm_bucket_artist_genre_jazz").alias("overall_avg_bpm_bucket_artist_genre_jazz")))
    .filter(F.col("bpm_bucket_artist_genre_jazz") > F.col("overall_avg_bpm_bucket_artist_genre_jazz"))
    .drop("overall_avg_bpm_bucket_artist_genre_jazz")
)

ranked = (
    with_activity
    .select(
        "release_date_ym",
        "duration_sec_is_explicit_true",
        "median_bpm_bucket",
        "median_bpm_bucket_filled",
        "release_date_ym_len",
        F.lag("release_date_ym_len").over(w_release_date_ym_len_by_media).alias("prev_release_date_ym_len"),
        F.round(F.avg("bpm_bucket_artist_genre_jazz").over(w_release_date_ym_len), 2).alias("release_date_ym_len_avg_bpm_bucket_artist_genre_"),
        F.count(F.lit(1)).over(w_release_date_ym_len).alias("rows_in_release_date_ym_len"),
        F.lag("release_date_ym_len").over(w_release_date_ym_len_by_durat).alias("prev_release_date_ym_len_2"),
        F.row_number().over(w_release_date_ym_len_by_media).alias("rn"),
    )
)

result = (
    ranked
    .filter(F.col("prev_release_date_ym_len_2") < 12)
    .select(
        "release_date_ym",
        "duration_sec_is_explicit_true",
        "median_bpm_bucket_filled",
        "release_date_ym_len",
        "release_date_ym_len_avg_bpm_bucket_artist_genre_",
        "rows_in_release_date_ym_len",
        "prev_release_date_ym_len_2",
        "rn",
        (F.col("median_bpm_bucket_filled") - F.col("prev_release_date_ym_len")).alias("median_bpm_bucket_filled_minus_prev_release_date"),
        F.when(F.col("median_bpm_bucket_filled") >= 138, "large").when(F.col("median_bpm_bucket_filled") >= 102, "medium").otherwise("small").alias("median_bpm_bucket_filled_tier"),
    )
    .orderBy(F.col("release_date_ym_len").asc_nulls_last(), "release_date_ym")
    .limit(20)
)
