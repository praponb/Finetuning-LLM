from pyspark.sql import functions as F

valid_artists = (
    df_artists
    .filter((F.lower(F.col("artist_name")).like("%gr%")) & (F.col("genre").isin("Indie", "Pop")))
    .select(
        "id",
        "artist_name",
        "genre",
        "is_verified",
        F.abs(F.col("debut_year") - 2006).alias("debut_year_distance"),
        F.when(F.col("debut_year") >= 2016, "A").when(F.col("debut_year") >= 1996, "B").otherwise("C").alias("debut_year_tier"),
        F.when(F.col("genre").isin("Pop", "Indie"), "group_a").when(F.col("genre") == "Classical", "group_b").otherwise("other").alias("genre_group"),
    )
)

with_activity = (
    valid_artists
    .crossJoin(valid_artists.agg(F.min("debut_year_distance").alias("overall_min_debut_year_distance")))
    .filter(F.col("debut_year_distance") > F.col("overall_min_debut_year_distance"))
    .drop("overall_min_debut_year_distance")
)

result = (
    with_activity
    .rollup("genre_group", "genre", "is_verified")
    .agg(
        F.min("debut_year_distance").alias("min_debut_year_distance"),
        F.array_sort(F.collect_set("genre")).alias("genre_list"),
    )
    .orderBy(
        F.col("genre_group").asc_nulls_last(),
        F.col("genre").asc_nulls_last(),
        F.col("is_verified").asc_nulls_last(),
    )
)
