from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm").asc_nulls_last(), "id")
w_artist_id_is_explicit_by_bpm_2 = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").desc())
w_artist_id_is_explicit_by_bpm_3 = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm").asc_nulls_last())
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy("duration_sec", "id")

filtered = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) | (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.max("duration_sec").over(w_is_explicit).alias("is_explicit_max_duration_sec"),
        F.sum("bpm").over(w_is_explicit).alias("is_explicit_sum_bpm"),
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_is_explicit_by_bpm)).alias("bpm_change"),
        F.round(F.avg("bpm").over(w_artist_id_is_explicit_by_bpm_2), 2).alias("bpm_avg_2_prior"),
        F.round(F.cume_dist().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_cume_dist"),
        F.round(F.cume_dist().over(w_artist_id_is_explicit_by_bpm_3), 3).alias("bpm_cume_dist"),
        F.round(F.cume_dist().over(w_artist_id_by_duration_sec), 3).alias("duration_sec_cume_dist_2"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit), F.lit(0)), 4).alias("bpm_share_of_is_explicit"),
    )
)

rollup_base = (
    windowed
    .groupBy("is_explicit", "distinct_artist_id_in_is_explicit")
    .agg(
        F.sum(F.when(F.col("is_explicit"), F.col("bpm"))).alias("bpm_is_explicit_true"),
        F.round(F.sum("bpm_cume_dist") / F.nullif(F.sum("is_explicit_max_duration_sec"), F.lit(0)), 3).alias("bpm_cume_dist_to_is_explicit_max_duration_sec_ra"),
        F.min("release_date").alias("first_release_date"),
        F.sum("duration_sec_cume_dist").alias("sum_duration_sec_cume_dist_check"),
    )
    .filter(F.col("sum_duration_sec_cume_dist_check") > 0.3)
    .drop("sum_duration_sec_cume_dist_check")
)

result = (
    rollup_base
    .filter(F.col("first_release_date").between(F.lit("2024-06-03").cast("date"), F.lit("2025-03-29").cast("date")))
    .select(
        "is_explicit",
        "distinct_artist_id_in_is_explicit",
        "bpm_is_explicit_true",
        "bpm_cume_dist_to_is_explicit_max_duration_sec_ra",
        "first_release_date",
        F.month(F.col("first_release_date")).alias("first_release_date_month"),
        F.date_format(F.col("first_release_date"), "yyyy-MM").alias("first_release_date_label"),
    )
    .orderBy(
        F.col("bpm_cume_dist_to_is_explicit_max_duration_sec_ra").asc_nulls_last(),
        "is_explicit",
        "distinct_artist_id_in_is_explicit",
    )
    .limit(20)
)
