from pyspark.sql import functions as F

scoped = (
    df_artists
    .filter((F.upper(F.trim(F.col("artist_name"))).like("L%")) & (F.col("is_verified")))
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        F.when(F.col("debut_year") >= 2018, "large").when(F.col("debut_year") >= 1997, "medium").otherwise("small").alias("debut_year_tier"),
    )
)

enriched = (
    scoped
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("country")), " ", "_")).alias("country_slug"),
        F.concat(F.col("artist_name"), F.lit("#"), F.col("debut_year").cast("string")).alias("artist_name_ref"),
    )
)

labeled = (
    enriched
    .select(
        "id",
        "debut_year",
        "country_slug",
        "artist_name_ref",
        F.lower(F.regexp_replace(F.trim(F.col("debut_year_tier")), " ", "_")).alias("debut_year_tier_slug"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.min("duration_sec").alias("min_track_duration_sec"),
        F.count("*").alias("track_count"),
        F.count("*").alias("track_count_2"),
    )
)

with_counts = (
    labeled.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.min_track_duration_sec",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        F.coalesce(F.col("t.track_count_2"), F.lit(0)).alias("track_count_2"),
    )
)

result = (
    with_counts
    .groupingSets(
        [["track_count", "track_count_2"], ["track_count"], ["track_count_2"]],
        "track_count", "track_count_2",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("debut_year", 0.5).alias("median_debut_year"),
    )
    .orderBy(
        F.col("track_count").asc_nulls_last(),
        F.col("track_count_2").asc_nulls_last(),
    )
)
