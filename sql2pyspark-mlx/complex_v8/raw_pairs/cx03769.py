from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-03-08").cast("date")) & (F.col("bpm") > 75))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        F.trunc(F.col("release_date"), "MM").alias("release_date_month_start"),
    )
)

rollup_base = (
    scoped_tracks
    .groupBy(
        F.date_trunc("month", "release_date_month_start").alias("release_date_month_start_month"),
    )
    .agg(
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.upper(F.trim(F.col("title"))).like("M%"), 1)).alias("n_upper_trim_title_like_m"),
        F.round(100 * F.count(F.when(F.length(F.trim(F.col("title"))) >= 8, 1)) / F.count("*"), 1).alias("pct_length_trim_title_8"),
    )
)

enriched = (
    rollup_base
    .select(
        "*",
        F.when(F.col("distinct_artist_ids") >= 8, "HIGH").when(F.col("distinct_artist_ids") >= 4, "MEDIUM").otherwise("LOW").alias("distinct_artist_ids_tier"),
        (F.col("cnt") - F.col("pct_length_trim_title_8")).alias("cnt_minus_pct_length_trim_title_8"),
    )
)

result = (
    enriched
    .filter(F.col("pct_length_trim_title_8") >= 45.0)
    .select(
        "release_date_month_start_month",
        "distinct_artist_ids",
        "cnt",
        "pct_length_trim_title_8",
        "distinct_artist_ids_tier",
        "cnt_minus_pct_length_trim_title_8",
    )
    .orderBy(
        F.col("cnt").desc_nulls_first(),
        "distinct_artist_ids_tier",
        "release_date_month_start_month",
    )
    .limit(10)
)
