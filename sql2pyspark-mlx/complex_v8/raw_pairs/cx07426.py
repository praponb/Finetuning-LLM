from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec_mo = Window.partitionBy("artist_id").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w_artist_id_by_duration_sec_ti = Window.partitionBy("artist_id").orderBy("duration_sec")
w_all_by_bpm_distance = Window.orderBy("bpm_distance", "id")

filtered = (
    df_tracks
    .filter(F.col("artist_id") <= 7)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("bpm") - 94).alias("bpm_distance"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "bpm_distance",
        F.round(F.avg("bpm").over(w_artist_id_by_duration_sec_mo), 2).alias("bpm_avg_3"),
        F.round(F.cume_dist().over(w_artist_id_by_duration_sec_ti), 3).alias("duration_sec_cume_dist"),
        F.dense_rank().over(w_all_by_bpm_distance).alias("bpm_distance_dense_rank"),
    )
)

with_activity = (
    sequenced.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country") == "BR"), "left_semi")
)

result = (
    with_activity
    .rollup("bpm_distance_dense_rank", "is_explicit")
    .agg(
        F.grouping("bpm_distance_dense_rank").alias("is_total_bpm_distance_dense_rank"),
        F.min("bpm_avg_3").alias("min_bpm_avg_3"),
        F.percentile_approx("duration_sec", 0.5).alias("median_duration_sec"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("bpm_distance_dense_rank").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
