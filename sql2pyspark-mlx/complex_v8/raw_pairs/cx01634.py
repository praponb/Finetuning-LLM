from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy("bpm", "id")
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id")
w_is_explicit_by_bpm_ties = Window.partitionBy("is_explicit").orderBy("bpm")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_all_by_duration_sec_per_bpm = Window.orderBy(F.col("duration_sec_per_bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_per_bpm_2 = Window.orderBy(F.col("duration_sec_per_bpm").desc_nulls_first())
w_is_explicit = Window.partitionBy("is_explicit")
w_all_by_duration_sec_per_bpm_3 = Window.orderBy(F.col("duration_sec_per_bpm").desc_nulls_first(), "id")

cleaned = (
    df_tracks
    .filter(F.col("duration_sec") <= 402)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        "duration_sec_bucket",
        "duration_sec_per_bpm",
        F.lead("bpm", 2).over(w_is_explicit_by_bpm).alias("next2_bpm"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_by_release_date)).alias("bpm_change"),
        F.round(F.cume_dist().over(w_is_explicit_by_bpm_ties), 3).alias("bpm_cume_dist"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id_is_explicit), 2).alias("bpm_vs_artist_id_is_explicit_avg"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.min("bpm").over(w_all_by_duration_sec_per_bpm).alias("min_bpm_so_far"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_per_bpm_2), 3).alias("duration_sec_per_bpm_percent_rank"),
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.round(F.col("duration_sec_per_bpm") - F.avg("duration_sec_per_bpm").over(w_artist_id_is_explicit), 2).alias("duration_sec_per_bpm_vs_artist_id_is_explicit_av"),
        F.dense_rank().over(w_all_by_duration_sec_per_bpm_3).alias("duration_sec_per_bpm_dense_rank"),
        F.round(F.col("duration_sec_bucket") - F.avg("duration_sec_bucket").over(w_is_explicit), 2).alias("duration_sec_bucket_vs_is_explicit_avg"),
    )
)

result = (
    metrics
    .select(
        "id",
        "is_explicit",
        "moods",
        "duration_sec_bucket",
        "duration_sec_per_bpm",
        "next2_bpm",
        "bpm_change",
        "bpm_cume_dist",
        "min_bpm_so_far",
        "duration_sec_per_bpm_percent_rank",
        "distinct_artist_id_in_is_explicit",
        "duration_sec_per_bpm_dense_rank",
        "duration_sec_bucket_vs_is_explicit_avg",
        F.least(F.col("bpm_cume_dist"), F.col("duration_sec_bucket")).alias("least_bpm_cume_dist_duration_sec_bucket"),
    )
    .orderBy(F.col("duration_sec_per_bpm").desc_nulls_first(), "id")
    .limit(5)
)
