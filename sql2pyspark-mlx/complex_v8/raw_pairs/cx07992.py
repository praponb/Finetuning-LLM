from pyspark.sql import functions as F

cleaned = (
    df_artists
    .filter(F.upper(F.trim(F.col("artist_name"))).like("N%"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
    )
)

stats = (
    cleaned
    .groupBy("country", "is_verified_flag")
    .agg(
        F.max("debut_year").alias("max_debut_year"),
        F.sum("debut_year").alias("total_debut_year"),
        F.array_sort(F.collect_set("genre")).alias("genre_list"),
        F.count(F.when(F.col("genre").isNotNull(), 1)).alias("n_genre_is_not_null"),
    )
)

result = (
    stats
    .select(
        "country",
        "is_verified_flag",
        "max_debut_year",
        "n_genre_is_not_null",
    )
    .unpivot(
        ids=["country", "is_verified_flag"],
        values=["max_debut_year", "n_genre_is_not_null"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("country", "is_verified_flag", "metric_name")
)
