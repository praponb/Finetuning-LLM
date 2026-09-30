from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter(F.year(F.col("release_date")) == 2024)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

scored = (
    recent_tracks
    .select(
        "*",
        F.month(F.col("release_date")).alias("release_date_month"),
        F.concat(F.col("is_explicit_label"), F.lit("#"), F.col("release_date_quarter").cast("string")).alias("is_explicit_label_ref"),
    )
)

with_details_artist = (
    scored.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.release_date_quarter",
        "s.is_explicit_label",
        "s.release_date_month",
        "s.is_explicit_label_ref",
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

stats = (
    with_details_artist
    .groupBy("release_date_month", "artist_country", "is_explicit")
    .agg(
        F.min("release_date").alias("first_release_date"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.length(F.trim(F.col("artist_name"))) >= 8, 1)).alias("n_length_trim_artist_name_8"),
        F.max("release_date").alias("last_release_date"),
    )
)

screened = (
    stats
    .crossJoin(stats.agg(F.avg("distinct_ids").alias("overall_avg_distinct_ids")))
    .filter(F.col("distinct_ids") > F.col("overall_avg_distinct_ids"))
    .drop("overall_avg_distinct_ids")
)

without_matches = (
    screened
    .crossJoin(screened.agg(F.avg("n_length_trim_artist_name_8").alias("overall_avg_n_length_trim_artist_name_8")))
    .filter(F.col("n_length_trim_artist_name_8") > F.col("overall_avg_n_length_trim_artist_name_8"))
    .drop("overall_avg_n_length_trim_artist_name_8")
)

result = (
    without_matches
    .filter(F.col("first_release_date") < F.lit("2025-08-07").cast("date"))
    .select(
        "release_date_month",
        "artist_country",
        "is_explicit",
        "first_release_date",
        "cnt",
        "distinct_ids",
        "last_release_date",
        F.coalesce(F.col("artist_country"), F.lit("UNKNOWN")).alias("artist_country_or_unknown"),
    )
)
