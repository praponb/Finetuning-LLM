from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("bpm_distance_bucket").desc(), F.col("id").desc())
w_artist_id_by_release_date_ti = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc())
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id")
w_all_by_release_date_full = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date_ties = Window.orderBy("release_date")

filtered_tracks = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 2) & (F.col("duration_sec") > 213))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("bpm") - 104).alias("bpm_distance"),
    )
)

annotated = (
    filtered_tracks
    .filter(F.col("bpm_distance") <= 162)
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        (F.floor(F.col("bpm_distance") / 25) * 25).alias("bpm_distance_bucket"),
    )
)

unique_rows = (
    annotated
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

ranked = (
    unique_rows
    .select(
        "id",
        "artist_id",
        "release_date",
        "moods",
        F.round(F.cume_dist().over(w_artist_id_by_release_date_ti), 3).alias("release_date_cume_dist"),
        F.round(F.col("bpm_distance_bucket") - F.avg("bpm_distance_bucket").over(w_artist_id_is_explicit), 2).alias("bpm_distance_bucket_vs_artist_id_is_explicit_avg"),
        F.ntile(4).over(w_artist_id_by_release_date).alias("release_date_quartile"),
        F.sum("bpm_distance_bucket").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_sum_bpm_distance_bucket"),
        F.nth_value("release_date", 2).over(w_all_by_release_date_full).alias("second_release_date"),
        F.dense_rank().over(w_all_by_release_date_ties).alias("release_date_dense_rank"),
    )
)

first_rows = (
    ranked
    .filter(F.col("release_date_dense_rank") <= 2)
    .drop("release_date_dense_rank")
)

without_matches = (
    first_rows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left_semi")
)

derived = (
    without_matches
    .filter(F.col("bpm_distance_bucket_vs_artist_id_is_explicit_avg") <= 126.0)
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("second_release_date")).alias("days_since_second_release_date"),
        F.date_add(F.col("release_date"), 30).alias("release_date_plus_30d"),
        F.datediff(F.col("second_release_date"), F.col("release_date")).alias("days_release_date_to_second_release_date"),
    )
)

result = (
    derived
    .select(
        "id",
        "release_date_cume_dist",
        "bpm_distance_bucket_vs_artist_id_is_explicit_avg",
        "artist_id_is_explicit_sum_bpm_distance_bucket",
        "second_release_date",
        "release_date_plus_30d",
        "days_release_date_to_second_release_date",
    )
    .orderBy(
        F.col("release_date_cume_dist").asc(),
        "days_release_date_to_second_release_date",
    )
)
