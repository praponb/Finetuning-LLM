from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm").desc(), "id")
w_all_by_release_date_full = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_year = Window.partitionBy(F.year("release_date"))
w_all_by_release_date_moving_6 = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_bpm_full = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm = Window.orderBy("bpm", "id")

recent = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.col("duration_sec") > 144))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
)

combined_details = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        F.col("a.country").alias("artist_country"),
    )
)

with_trends = (
    combined_details
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.dense_rank().over(w_artist_id_is_explicit_by_bpm).alias("bpm_dense_rank"),
        F.nth_value("release_date", 2).over(w_all_by_release_date_full).alias("second_release_date"),
        F.count(F.lit(1)).over(w_release_date_year).alias("rows_in_release_date_year"),
        F.round(F.avg("bpm").over(w_all_by_release_date_moving_6), 2).alias("bpm_avg_7"),
        F.nth_value("bpm", 2).over(w_all_by_bpm_full).alias("second_bpm"),
        F.sum("bpm").over(w_all_by_release_date_moving_6).alias("bpm_sum_7"),
        F.row_number().over(w_all_by_bpm).alias("rn"),
        F.round(F.cume_dist().over(w_artist_id_is_explicit_by_bpm), 3).alias("bpm_cume_dist"),
        F.size(F.collect_set("artist_id").over(w_release_date_year)).alias("distinct_artist_id_in_release_date_year"),
    )
)

top_per_group = (
    with_trends
    .filter(F.col("rn") <= 3)
    .drop("rn")
)

result = (
    top_per_group
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "second_release_date",
        "bpm_avg_7",
        "second_bpm",
        "bpm_sum_7",
        "bpm_cume_dist",
        "distinct_artist_id_in_release_date_year",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("second_release_date")).alias("days_since_second_release_date"),
    )
    .orderBy(
        F.col("is_explicit_label").desc(),
        F.col("bpm_cume_dist").asc_nulls_last(),
    )
)
