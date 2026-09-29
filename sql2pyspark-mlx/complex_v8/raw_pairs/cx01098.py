from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("duration_sec") - 279).alias("duration_sec_distance"),
        F.add_months(F.col("release_date"), 12).alias("release_date_plus_12m"),
    )
)

summary = (
    prepared
    .groupBy(
        "artist_id",
        F.date_format("release_date", "yyyy-MM").alias("release_date_ym"),
        F.date_format("release_date_plus_12m", "yyyy-MM").alias("release_date_plus_12m_ym"),
    )
    .agg(
        F.sum(F.when(F.col("is_explicit"), 1).otherwise(0)).alias("n_is_explicit_true"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.count("*").alias("n_rows"),
        F.round(F.avg("bpm"), 2).alias("avg_bpm"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    summary
    .select(
        "artist_id",
        "release_date_plus_12m_ym",
        "avg_bpm",
        "distinct_ids",
        F.concat(F.col("release_date_ym"), F.lit("#"), F.col("n_is_explicit_true").cast("string")).alias("release_date_ym_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("release_date_ym")), " ", "_")).alias("release_date_ym_slug"),
    )
    .orderBy(
        F.col("release_date_ym_slug").desc_nulls_first(),
        F.col("release_date_ym_ref").asc(),
    )
)
