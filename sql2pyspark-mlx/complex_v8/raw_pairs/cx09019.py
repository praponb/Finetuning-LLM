from pyspark.sql import functions as F

base = (
    df_artists
    .filter(~F.col("is_verified"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        (F.floor(F.col("debut_year") / 5) * 5).alias("debut_year_bucket"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.min("release_date").alias("first_track_release_date"),
        F.count("*").alias("track_count"),
    )
)

with_counts = (
    base.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_track_release_date",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

rollup_base = (
    with_counts
    .filter(F.col("track_count").between(0, 8))
    .groupBy(
        "genre",
        "country",
        F.year("first_track_release_date").alias("first_track_release_date_year"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "JP", 1).otherwise(0)).alias("n_country_jp"),
        F.array_sort(F.collect_set("country")).alias("country_list"),
        F.sum("debut_year").alias("sum_debut_year_check"),
    )
    .filter(F.col("sum_debut_year_check") > 1996)
    .drop("sum_debut_year_check")
)

screened = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("first_track_release_date_year").alias("overall_avg_first_track_release_date_year")))
    .filter(F.col("first_track_release_date_year") > F.col("overall_avg_first_track_release_date_year"))
    .drop("overall_avg_first_track_release_date_year")
)

result = (
    screened
    .select(
        "genre",
        "country",
        "first_track_release_date_year",
        "country_list",
        F.coalesce(F.col("n_country_jp"), F.lit(0)).alias("n_country_jp_filled"),
    )
    .orderBy("genre", F.col("country").asc())
)
