from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").asc_nulls_last())
w_artist_id_by_duration_sec_ti = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc())
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_tracks = (
    df_tracks
    .filter(F.col("artist_id") >= 3)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
    )
)

with_activity = (
    cleaned_tracks
    .crossJoin(cleaned_tracks.agg(F.min("duration_sec").alias("overall_min_duration_sec")))
    .filter(F.col("duration_sec") > F.col("overall_min_duration_sec"))
    .drop("overall_min_duration_sec")
)

metrics = (
    with_activity
    .select(
        "*",
        F.first("duration_sec").over(w_is_explicit_artist_id_by_dur).alias("first_duration_sec"),
        F.row_number().over(w_artist_id_by_duration_sec).alias("rn"),
        F.round(F.percent_rank().over(w_is_explicit_by_duration_sec), 3).alias("duration_sec_percent_rank"),
        F.rank().over(w_artist_id_by_duration_sec_ti).alias("duration_sec_rank"),
        F.max("duration_sec").over(w_is_explicit_by_duration_sec_2).alias("max_duration_sec_so_far"),
    )
)

tagged = (
    metrics
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods_text",
        "first_duration_sec",
        "rn",
        "duration_sec_percent_rank",
        "duration_sec_rank",
        "max_duration_sec_so_far",
        F.month(F.col("release_date")).alias("release_date_month"),
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
    )
)

grouped = (
    tagged
    .groupBy("rn")
    .agg(
        F.sum(F.when(F.col("is_explicit"), 1).otherwise(0)).alias("n_is_explicit_true"),
        F.min("first_duration_sec").alias("min_first_duration_sec"),
        F.percentile_approx("duration_sec", 0.5).alias("median_duration_sec"),
    )
)

screened = (
    grouped
    .crossJoin(grouped.agg(F.avg("min_first_duration_sec").alias("overall_avg_min_first_duration_sec")))
    .filter(F.col("min_first_duration_sec") > F.col("overall_avg_min_first_duration_sec"))
    .drop("overall_avg_min_first_duration_sec")
)

annotated = (
    screened
    .select(
        "rn",
        "n_is_explicit_true",
        "min_first_duration_sec",
        F.coalesce(F.col("median_duration_sec"), F.lit(0)).alias("median_duration_sec_filled"),
        F.when(F.col("median_duration_sec") >= 338, "HIGH").when(F.col("median_duration_sec") >= 172, "MEDIUM").otherwise("LOW").alias("median_duration_sec_tier"),
    )
)

result = (
    annotated
    .cube("rn", "n_is_explicit_true")
    .agg(
        F.percentile_approx("median_duration_sec_filled", 0.5).alias("median_median_duration_sec_filled"),
        F.countDistinct("median_duration_sec_tier").alias("distinct_median_duration_sec_tiers"),
    )
    .orderBy(
        F.col("rn").asc_nulls_last(),
        F.col("n_is_explicit_true").asc_nulls_last(),
    )
)
