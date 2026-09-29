from pyspark.sql import functions as F

cleaned_artists = (
    df_artists
    .filter((F.col("debut_year").between(1990, 2006)) & (F.col("genre") == "Indie"))
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        "is_verified",
        F.concat(F.col("country"), F.lit("#"), F.col("debut_year").cast("string")).alias("country_ref"),
        F.concat(F.col("genre"), F.lit("#"), F.col("id").cast("string")).alias("genre_ref"),
        F.concat_ws(" / ", F.col("country"), F.col("genre")).alias("country_genre_label"),
    )
)

scored = (
    cleaned_artists
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        "country_ref",
        "genre_ref",
        "country_genre_label",
        F.concat_ws("|", F.col("country"), F.col("artist_name")).alias("country_artist_name_label"),
        F.when(F.col("debut_year") >= 2015, "large").when(F.col("debut_year") >= 1997, "medium").otherwise("small").alias("debut_year_tier"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)

with_activity = (
    scored
    .crossJoin(scored.agg(F.avg("debut_year").alias("overall_avg_debut_year")))
    .filter(F.col("debut_year") > F.col("overall_avg_debut_year"))
    .drop("overall_avg_debut_year")
)

derived = with_activity.withColumn("debut_year_filled", F.coalesce(F.col("debut_year"), F.lit(0)))

result = (
    derived
    .groupingSets(
        [["debut_year_tier", "country", "country_or_unknown"], ["debut_year_tier"]],
        "debut_year_tier", "country", "country_or_unknown",
    )
    .agg(F.countDistinct("country").alias("distinct_countrys"))
    .select(
        F.coalesce(F.col("debut_year_tier"), F.lit("ALL")).alias("debut_year_tier"),
        F.coalesce(F.col("country"), F.lit("ALL")).alias("country"),
        F.coalesce(F.col("country_or_unknown"), F.lit("ALL")).alias("country_or_unknown"),
        "distinct_countrys",
    )
    .orderBy(
        F.col("debut_year_tier").asc_nulls_last(),
        F.col("country").asc_nulls_last(),
        F.col("country_or_unknown").asc_nulls_last(),
    )
)
