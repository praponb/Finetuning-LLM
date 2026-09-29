from pyspark.sql import functions as F

prepared = (
    df_artists
    .filter(F.col("genre") == "HipHop")
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

with_activity = (
    prepared
    .crossJoin(prepared.agg(F.min("debut_year").alias("overall_min_debut_year")))
    .filter(F.col("debut_year") > F.col("overall_min_debut_year"))
    .drop("overall_min_debut_year")
)

genre_stats = (
    with_activity
    .groupBy("genre")
    .agg(F.max("debut_year").alias("max_debut_year"))
)

outliers = (
    with_activity.alias("s")
    .join(genre_stats.alias("g"), F.col("g.genre") == F.col("s.genre"), "inner")
    .filter(F.col("s.debut_year") == F.col("g.max_debut_year"))
    .select(
        "s.*",
        "g.max_debut_year",
        F.round(F.col("s.debut_year") - F.col("g.max_debut_year"), 2).alias("debut_year_minus_max"),
    )
)

result = (
    outliers
    .filter(F.col("debut_year_minus_max") > -1011.5)
    .select(
        "id",
        "country",
        "debut_year",
        "is_verified",
        "is_verified_label",
        "max_debut_year",
        "debut_year_minus_max",
        F.concat(F.col("artist_name"), F.lit("#"), F.col("id").cast("string")).alias("artist_name_ref"),
    )
    .orderBy(F.col("id").desc(), F.col("artist_name_ref").asc())
)
