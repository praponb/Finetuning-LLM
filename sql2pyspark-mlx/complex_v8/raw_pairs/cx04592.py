from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter(F.col("release_date").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

labeled = (
    cleaned_tracks
    .filter(F.upper(F.trim(F.col("title_clean"))).like("C%"))
    .select(
        "*",
        F.add_months(F.col("release_date"), 1).alias("release_date_plus_1m"),
    )
)

result = (
    labeled
    .rollup("artist_id", "is_explicit")
    .agg(
        F.grouping_id().alias("gid"),
        F.max("duration_sec").alias("max_duration_sec"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
