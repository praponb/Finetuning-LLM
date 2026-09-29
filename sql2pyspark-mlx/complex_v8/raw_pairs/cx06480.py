from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter(F.col("release_date").isNotNull())
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        (F.col("bpm") + F.col("duration_sec")).alias("bpm_plus_duration_sec"),
        F.when(F.col("duration_sec") >= 312, "large").when(F.col("duration_sec") >= 164, "medium").otherwise("small").alias("duration_sec_tier"),
        F.year(F.col("release_date")).alias("release_date_year"),
    )
)

derived = (
    filtered_tracks
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("duration_sec_tier")), " ", "_")).alias("duration_sec_tier_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.when(F.col("release_date_year") >= 2026, "large").when(F.col("release_date_year") >= 2025, "medium").otherwise("small").alias("release_date_year_tier"),
    )
)

result = (
    derived
    .groupingSets(
        [["is_explicit", "release_date_year_tier"], ["is_explicit"]],
        "is_explicit", "release_date_year_tier",
    )
    .agg(
        F.grouping("is_explicit").alias("is_total_is_explicit"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
    )
    .select(
        "is_explicit",
        F.coalesce(F.col("release_date_year_tier"), F.lit("ALL")).alias("release_date_year_tier"),
        "is_total_is_explicit",
        "distinct_ids",
        "row_count",
    )
)
