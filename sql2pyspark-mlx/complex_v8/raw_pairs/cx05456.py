from pyspark.sql import functions as F

cleaned = (
    df_artists
    .filter((F.col("genre") == "Pop") & (F.upper(F.trim(F.col("artist_name"))).like("K%")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.when(F.col("is_verified"), "yes").otherwise("no").alias("is_verified_label"),
    )
)

genre_stats = (
    cleaned
    .groupBy("genre")
    .agg(F.max("debut_year").alias("max_debut_year"))
)

vs_group = (
    cleaned.alias("s")
    .join(genre_stats.alias("g"), F.col("g.genre") == F.col("s.genre"), "inner")
    .filter(F.col("s.debut_year") == F.col("g.max_debut_year"))
    .select(
        "s.*",
        "g.max_debut_year",
        F.round(F.col("s.debut_year") - F.col("g.max_debut_year"), 2).alias("debut_year_minus_max"),
    )
)

result = (
    vs_group
    .cube("is_verified", "genre")
    .agg(
        F.grouping("is_verified").alias("is_total_is_verified"),
        F.round(F.avg("debut_year_minus_max"), 2).alias("avg_debut_year_minus_max"),
    )
    .orderBy(F.col("is_verified").asc_nulls_last(), F.col("genre").asc_nulls_last())
)
