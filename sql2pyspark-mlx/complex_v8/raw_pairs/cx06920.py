from pyspark.sql import functions as F

cleaned_artists = (
    df_artists
    .filter(F.col("debut_year") < 2015)
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        (F.floor(F.col("debut_year") / 5) * 5).alias("debut_year_bucket"),
        F.when(F.col("debut_year") >= 2016, "A").when(F.col("debut_year") >= 1994, "B").otherwise("C").alias("debut_year_tier"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.min("release_date").alias("first_track_release_date"),
        F.min("release_date").alias("first_track_release_date_2"),
        F.count("*").alias("track_count"),
    )
)

with_stats = (
    cleaned_artists.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_track_release_date",
        "t.first_track_release_date_2",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

result = (
    with_stats
    .rollup("debut_year_tier", "country_or_unknown")
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("genre")).alias("genre_list"),
    )
)
