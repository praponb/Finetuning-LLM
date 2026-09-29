from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-08-02").cast("date")) | (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
    )
)

annotated = (
    recent_tracks
    .select(
        "id",
        "title",
        "is_explicit",
        "release_date_quarter",
        "title_ref",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.hour(F.col("release_date_quarter")).alias("release_date_quarter_hour"),
    )
)

scored = (
    annotated
    .select(
        "*",
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
        F.date_trunc("month", F.col("release_date_quarter")).alias("release_date_quarter_month"),
    )
)

filtered = (
    scored.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.is_skipped")), "left_anti")
)

result = (
    filtered
    .cube("is_explicit", "is_explicit_label", "release_date_quarter_hour")
    .agg(
        F.max("release_date_quarter_hour").alias("max_release_date_quarter_hour"),
        F.round(100 * F.count(F.when(F.col("title").isNotNull(), 1)) / F.count("*"), 1).alias("pct_title_is_not_null"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("is_explicit_label").asc_nulls_last(),
        F.col("release_date_quarter_hour").asc_nulls_last(),
    )
)
