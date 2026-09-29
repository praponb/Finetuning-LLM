from pyspark.sql import functions as F

base_artists = (
    df_artists
    .filter((~F.col("is_verified")) & (F.col("country").isNotNull()))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
        F.concat_ws(": ", F.col("genre"), F.col("country")).alias("genre_country_label"),
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
    )
)

filtered = (
    base_artists
    .crossJoin(base_artists.agg(F.avg("is_verified_flag").alias("overall_avg_is_verified_flag")))
    .filter(F.col("is_verified_flag") > F.col("overall_avg_is_verified_flag"))
    .drop("overall_avg_is_verified_flag")
)

result = (
    filtered
    .groupingSets(
        [["is_verified_flag", "genre", "is_verified"], ["is_verified_flag"], []],
        "is_verified_flag", "genre", "is_verified",
    )
    .agg(
        F.sum(F.when(F.col("artist_name").isNotNull(), F.col("debut_year")).otherwise(0)).alias("debut_year_artist_name_is_not_null"),
        F.sum(F.when(F.col("genre") == "Indie", 1).otherwise(0)).alias("n_genre_indie"),
        F.round(F.avg("debut_year"), 2).alias("avg_debut_year"),
    )
    .select(
        "is_verified_flag",
        F.coalesce(F.col("genre"), F.lit("ALL")).alias("genre"),
        "is_verified",
        "debut_year_artist_name_is_not_null",
        "n_genre_indie",
        "avg_debut_year",
    )
    .orderBy(
        F.col("is_verified_flag").asc_nulls_last(),
        F.col("genre").asc_nulls_last(),
        F.col("is_verified").asc_nulls_last(),
    )
)
