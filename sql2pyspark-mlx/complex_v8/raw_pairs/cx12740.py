from pyspark.sql import functions as F

base = (
    df_tracks
    .filter((F.col("title").isNotNull()) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
    )
)

per_group = (
    base
    .groupBy("is_explicit")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.upper(F.trim(F.col("title"))).like("N%"), F.col("bpm_per_duration_sec"))).alias("bpm_per_duration_sec_upper_trim_title_like_n"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.sum("bpm_per_duration_sec").alias("total_bpm_per_duration_sec"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 3)
)

enriched = (
    per_group
    .filter(F.col("total_bpm_per_duration_sec") >= 21.0)
    .select(
        "*",
        F.round(F.col("distinct_ids") / F.nullif(F.col("row_count"), F.lit(0)), 3).alias("distinct_ids_per_row_count"),
        (F.col("distinct_ids") + F.col("row_count")).alias("distinct_ids_plus_row_count"),
        (F.floor(F.col("stddev_duration_sec") / 100) * 100).alias("stddev_duration_sec_bucket"),
    )
)

derived = (
    enriched
    .filter(F.col("stddev_duration_sec_bucket") < 357)
    .select(
        "*",
        (F.col("distinct_ids") - F.col("stddev_duration_sec")).alias("distinct_ids_minus_stddev_duration_sec"),
        F.when(F.col("row_count") >= 8, "A").when(F.col("row_count") >= 4, "B").otherwise("C").alias("row_count_tier"),
        F.round(F.col("distinct_ids") / F.nullif(F.col("row_count"), F.lit(0)), 3).alias("distinct_ids_per_row_count_2"),
    )
)

result = (
    derived
    .select(
        "is_explicit",
        "stddev_duration_sec",
        "total_bpm_per_duration_sec",
        "row_count",
        "distinct_ids_per_row_count",
        "stddev_duration_sec_bucket",
        "distinct_ids_minus_stddev_duration_sec",
        "row_count_tier",
        "distinct_ids_per_row_count_2",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
    .orderBy(F.col("total_bpm_per_duration_sec").asc())
)
