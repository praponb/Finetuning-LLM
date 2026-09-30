from pyspark.sql import functions as F

scoped = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
        F.add_months(F.col("release_date"), 12).alias("release_date_plus_12m"),
    )
)

screened = (
    scoped.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.size(F.col("t.moods")) >= 2), "left_semi")
    .filter(F.col("artist_id") <= 6)
)

with_parent = (
    screened.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.release_date_quarter",
        "s.release_date_plus_12m",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

stats = (
    with_parent
    .filter(F.col("artist_country") == "KR")
    .groupBy(
        F.date_format("release_date_plus_12m", "yyyy-MM").alias("release_date_plus_12m_ym"),
        "release_date_quarter",
    )
    .agg(
        F.percentile_approx("artist_debut_year", 0.5).alias("median_artist_debut_year"),
        F.sum(F.when(~F.col("is_explicit"), F.col("release_date_quarter")).otherwise(0)).alias("release_date_quarter_not_is_explicit"),
        F.count("*").alias("row_count"),
    )
)

labeled = (
    stats
    .select(
        "release_date_plus_12m_ym",
        "release_date_quarter",
        "median_artist_debut_year",
        "release_date_quarter_not_is_explicit",
        "row_count",
        (F.col("row_count") - F.col("median_artist_debut_year")).alias("row_count_minus_median_artist_debut_year"),
        F.initcap(F.trim(F.col("release_date_plus_12m_ym"))).alias("release_date_plus_12m_ym_clean"),
    )
)

filtered = (
    labeled
    .crossJoin(labeled.agg(F.avg("median_artist_debut_year").alias("overall_avg_median_artist_debut_year")))
    .filter(F.col("median_artist_debut_year") > F.col("overall_avg_median_artist_debut_year"))
    .drop("overall_avg_median_artist_debut_year")
)

enriched = (
    filtered
    .select(
        "release_date_plus_12m_ym",
        "release_date_quarter",
        "median_artist_debut_year",
        "release_date_quarter_not_is_explicit",
        "row_count",
        "release_date_plus_12m_ym_clean",
        (F.floor(F.col("median_artist_debut_year") / 5) * 5).alias("median_artist_debut_year_bucket"),
    )
)

result = (
    enriched
    .select(
        "release_date_plus_12m_ym",
        "median_artist_debut_year",
        "row_count",
        "median_artist_debut_year_bucket",
    )
)
