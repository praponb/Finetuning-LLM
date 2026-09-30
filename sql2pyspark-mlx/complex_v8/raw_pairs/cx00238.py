from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_track_count_by_moo = Window.partitionBy("artist_id", "track_count").orderBy(F.col("moods_count_plus_duration_sec").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_track_count_by_moo_2 = Window.partitionBy("artist_id", "track_count").orderBy(F.col("moods_count_plus_duration_sec").desc_nulls_first(), "id")
w_all_by_min_track_duration_se = Window.orderBy("min_track_duration_sec", "id").rowsBetween(-6, -1)
w_all_by_min_track_duration_se_2 = Window.orderBy("min_track_duration_sec", "id").rowsBetween(-2, -1)

valid_tracks = (
    df_tracks
    .filter((F.year(F.col("release_date")) == 2024) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.size(F.col("moods")).alias("moods_count"),
    )
)

enriched = (
    valid_tracks
    .select(
        "*",
        F.round(F.col("duration_sec") / F.nullif(F.col("moods_count"), F.lit(0)), 3).alias("duration_sec_per_moods_count"),
        (F.col("duration_sec") - F.col("moods_count")).alias("duration_sec_minus_moods_count"),
        (F.col("moods_count") + F.col("duration_sec")).alias("moods_count_plus_duration_sec"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.min("duration_sec").alias("min_track_duration_sec"),
        F.min("release_date").alias("first_track_release_date"),
    )
)

summarized_rows = (
    enriched.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.min_track_duration_sec",
        "t.first_track_release_date",
    )
)

with_windows = (
    summarized_rows
    .select(
        "*",
        F.sum("duration_sec").over(w_artist_id_track_count_by_moo).alias("running_duration_sec"),
        F.row_number().over(w_artist_id_track_count_by_moo_2).alias("moods_count_plus_duration_sec_seq"),
        F.sum("min_track_duration_sec").over(w_all_by_min_track_duration_se).alias("min_track_duration_sec_sum_6_prior"),
        F.sum("duration_sec").over(w_all_by_min_track_duration_se_2).alias("duration_sec_sum_2_prior"),
    )
)

result = (
    with_windows
    .cube("moods_count", "is_explicit")
    .agg(
        F.grouping("moods_count").alias("is_total_moods_count"),
        F.round(F.sum("min_track_duration_sec") / F.nullif(F.sum("duration_sec"), F.lit(0)), 3).alias("min_track_duration_sec_to_duration_sec_ratio"),
    )
)
