from pyspark.sql import functions as F

cleaned = (
    df_artists
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.concat_ws("-", F.col("genre"), F.col("artist_name")).alias("genre_artist_name_label"),
    )
)

country_stats = (
    cleaned
    .groupBy("country")
    .agg(F.max("debut_year").alias("max_debut_year"))
)

vs_group = (
    cleaned.alias("s")
    .join(country_stats.alias("g"), F.col("g.country") == F.col("s.country"), "inner")
    .filter(F.col("s.debut_year") == F.col("g.max_debut_year"))
    .select(
        "s.*",
        "g.max_debut_year",
        F.round(F.col("s.debut_year") - F.col("g.max_debut_year"), 2).alias("debut_year_minus_max"),
    )
)

summary = (
    vs_group
    .groupBy("is_verified")
    .agg(
        F.max("max_debut_year").alias("max_max_debut_year"),
        F.count("*").alias("cnt"),
        F.round(F.sum("debut_year") / F.nullif(F.sum("max_debut_year"), F.lit(0)), 3).alias("debut_year_to_max_debut_year_ratio"),
        F.countDistinct("genre").alias("distinct_genres"),
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
    )
)

result = (
    summary
    .select(
        "is_verified",
        "cnt",
        "debut_year_to_max_debut_year_ratio",
        "distinct_genres",
        F.when(F.col("max_max_debut_year") >= 2013.1, "HIGH").when(F.col("max_max_debut_year") >= 2003.2, "MEDIUM").otherwise("LOW").alias("max_max_debut_year_tier"),
    )
)
