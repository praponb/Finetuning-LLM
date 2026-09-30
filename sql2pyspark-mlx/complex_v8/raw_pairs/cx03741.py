from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("bpm_tier", "artist_id").orderBy(F.col("release_date").desc(), F.col("id").desc())
w_latest = Window.partitionBy("bpm_tier").orderBy(F.col("duration_sec").desc(), F.col("id").desc())
w_bpm_tier_artist_id_by_releas = Window.partitionBy("bpm_tier", "artist_id").orderBy("release_date", "id")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-1, -1)
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_all_by_release_date = Window.orderBy("release_date", "id")

recent = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-03-29").cast("date")) | (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.when(F.col("bpm") >= 142, "A").when(F.col("bpm") >= 94, "B").otherwise("C").alias("bpm_tier"),
    )
)

deduped = (
    recent
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped_2 = (
    deduped
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

screened = (
    deduped_2.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.is_verified")), "left_semi")
)

with_trends = (
    screened
    .select(
        "*",
        F.round(F.cume_dist().over(w_bpm_tier_artist_id_by_releas), 3).alias("release_date_cume_dist"),
        F.round(F.avg("duration_sec").over(w_is_explicit_by_duration_sec), 2).alias("duration_sec_avg_1_prior"),
        F.size(F.collect_set("bpm_tier").over(w_is_explicit_artist_id)).alias("distinct_bpm_tier_in_is_explicit_artist_id"),
        F.max("duration_sec").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_max_duration_sec"),
        F.rank().over(w_all_by_release_date).alias("release_date_rank"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["distinct_bpm_tier_in_is_explicit_artist_id", "bpm_tier", "release_date_rank"], ["distinct_bpm_tier_in_is_explicit_artist_id"], ["bpm_tier"], []],
        "distinct_bpm_tier_in_is_explicit_artist_id", "bpm_tier", "release_date_rank",
    )
    .agg(
        F.grouping("distinct_bpm_tier_in_is_explicit_artist_id").alias("is_total_distinct_bpm_tier_in_is_explicit_artist"),
        F.min("distinct_bpm_tier_in_is_explicit_artist_id").alias("min_distinct_bpm_tier_in_is_explicit_artist_id"),
    )
    .orderBy(
        F.col("distinct_bpm_tier_in_is_explicit_artist_id").asc_nulls_last(),
        F.col("bpm_tier").asc_nulls_last(),
        F.col("release_date_rank").asc_nulls_last(),
    )
)
