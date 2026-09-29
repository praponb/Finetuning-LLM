from pyspark.sql import functions as F

prepared = (
    df_artists
    .filter((F.col("is_verified")) & (F.col("artist_name").isNotNull()))
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        F.lower(F.regexp_replace(F.trim(F.col("country")), " ", "_")).alias("country_slug"),
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
    )
)

scored = (
    prepared
    .select(
        "*",
        F.concat(F.col("country_slug"), F.lit("#"), F.col("debut_year_filled").cast("string")).alias("country_slug_ref"),
        F.greatest(F.col("debut_year"), F.col("debut_year_filled")).alias("greatest_debut_year_debut_year_filled"),
        F.abs(F.col("debut_year_filled") - 2003).alias("debut_year_filled_distance"),
    )
)

screened = (
    scored
    .crossJoin(scored.agg(F.max("debut_year_filled").alias("overall_max_debut_year_filled")))
    .filter(F.col("debut_year_filled") < F.col("overall_max_debut_year_filled"))
    .drop("overall_max_debut_year_filled")
)

result = (
    screened
    .filter(F.col("country_slug").isNotNull())
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        "country_slug",
        "debut_year_filled",
        "country_slug_ref",
        "greatest_debut_year_debut_year_filled",
        "debut_year_filled_distance",
        F.least(F.col("debut_year"), F.col("debut_year_filled")).alias("least_debut_year_debut_year_filled"),
    )
    .orderBy(F.col("country").asc(), "id")
    .limit(10)
)
