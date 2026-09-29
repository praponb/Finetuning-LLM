from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_row = Window.partitionBy("artist_id", "is_explicit").orderBy("row_count", "release_date_month_2", "artist_id", "is_explicit").rowsBetween(-1, Window.currentRow)
w_distinct_ids_distance_by_max = Window.partitionBy("distinct_ids_distance").orderBy("max_duration_sec", "release_date_month_2", "artist_id", "is_explicit").rowsBetween(-2, Window.currentRow)
w_distinct_ids_distance_by_max_2 = Window.partitionBy("distinct_ids_distance").orderBy("max_duration_sec")
w_artist_id_is_explicit_by_row_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("row_count", "release_date_month_2", "artist_id", "is_explicit").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_2_month = Window.partitionBy(F.month("release_date_month_2"))

cleaned_tracks = (
    df_tracks
    .filter(F.col("artist_id") <= 5)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("bpm") >= 142, "HIGH").when(F.col("bpm") >= 89, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
        (F.col("duration_sec") + F.col("bpm")).alias("duration_sec_plus_bpm"),
        F.month(F.col("release_date")).alias("release_date_month"),
    )
)

grouped = (
    cleaned_tracks
    .filter(F.col("duration_sec_plus_bpm") >= 303)
    .groupBy(
        F.date_trunc("month", "release_date").alias("release_date_month_2"),
        "artist_id",
        "is_explicit",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.max("duration_sec").alias("max_duration_sec"),
        F.min("bpm").alias("min_bpm"),
    )
    .filter(F.col("row_count") >= 3)
)

annotated = (
    grouped
    .select(
        "*",
        F.abs(F.col("distinct_ids") - 6).alias("distinct_ids_distance"),
        F.date_trunc("year", F.col("release_date_month_2")).alias("release_date_month_2_year"),
    )
)

ranked = (
    annotated
    .select(
        "*",
        F.round(F.avg("max_duration_sec").over(w_artist_id_is_explicit_by_row), 2).alias("max_duration_sec_avg_2"),
        F.max("max_duration_sec").over(w_distinct_ids_distance_by_max).alias("max_duration_sec_max_3"),
        F.round(F.cume_dist().over(w_distinct_ids_distance_by_max_2), 3).alias("max_duration_sec_cume_dist"),
        F.nth_value("min_bpm", 2).over(w_artist_id_is_explicit_by_row_2).alias("second_min_bpm"),
        F.sum("max_duration_sec").over(w_release_date_month_2_month).alias("release_date_month_2_month_sum_max_duration_sec"),
    )
)

result = (
    ranked
    .select(
        "release_date_month_2",
        "artist_id",
        "row_count",
        "min_bpm",
        "distinct_ids_distance",
        "release_date_month_2_year",
        "max_duration_sec_max_3",
        "max_duration_sec_cume_dist",
        "second_min_bpm",
        "release_date_month_2_month_sum_max_duration_sec",
        F.abs(F.col("distinct_ids_distance") - 4).alias("distinct_ids_distance_distance"),
    )
    .orderBy(
        F.col("distinct_ids_distance_distance").asc_nulls_last(),
        F.col("release_date_month_2_year").desc(),
    )
)
