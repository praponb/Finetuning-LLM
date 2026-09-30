from pyspark.sql import functions as F

recent = (
    df_artists
    .filter(F.col("country") != "TH")
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "is_verified",
        F.when(F.col("debut_year") >= 2018, "HIGH").when(F.col("debut_year") >= 1996, "MEDIUM").otherwise("LOW").alias("debut_year_tier"),
        F.substring(F.trim(F.col("country")), 1, 4).alias("country_prefix"),
        F.when(F.col("debut_year") >= 2006, "A").when(F.col("debut_year") >= 1997, "B").otherwise("C").alias("debut_year_tier_2"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.min("release_date").alias("first_track_release_date"),
        F.count("*").alias("track_count"),
        F.count("*").alias("track_count_2"),
    )
)

summarized_rows = (
    recent.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_track_release_date",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        F.coalesce(F.col("t.track_count_2"), F.lit(0)).alias("track_count_2"),
    )
)

enriched = (
    summarized_rows
    .select(
        "*",
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
        F.coalesce(F.col("track_count"), F.lit(0)).alias("track_count_filled"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.year(F.col("first_track_release_date")).alias("first_track_release_date_year"),
        F.substring(F.trim(F.col("country")), 1, 4).alias("country_prefix_2"),
        F.abs(F.col("track_count_filled") - 5).alias("track_count_filled_distance"),
    )
)

result = (
    scored
    .rollup("track_count_filled", "track_count_2")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum("track_count_filled_distance").alias("total_track_count_filled_distance"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("track_count_filled").asc_nulls_last(),
        F.col("track_count_2").asc_nulls_last(),
    )
)
