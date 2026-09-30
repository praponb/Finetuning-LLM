from pyspark.sql import functions as F

filtered_artists = (
    df_artists
    .filter(F.col("debut_year") <= 2006)
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        F.coalesce(F.col("genre"), F.lit("UNKNOWN")).alias("genre_or_unknown"),
        F.upper(F.trim(F.col("country"))).alias("country_clean"),
    )
)

totals = (
    filtered_artists
    .filter(F.col("genre_or_unknown") == "HipHop")
    .groupBy("genre")
    .agg(
        F.sum("debut_year").alias("total_debut_year"),
        F.count("*").alias("row_count"),
        F.countDistinct("genre").alias("distinct_genres"),
        F.sum(F.when(F.col("genre") == "Jazz", 1).otherwise(0)).alias("n_genre_jazz"),
    )
)

filtered = (
    totals
    .crossJoin(totals.agg(F.max("n_genre_jazz").alias("overall_max_n_genre_jazz")))
    .filter(F.col("n_genre_jazz") < F.col("overall_max_n_genre_jazz"))
    .drop("overall_max_n_genre_jazz")
)

result = (
    filtered
    .groupBy("row_count")
    .pivot("genre", ["HipHop", "Classical", "Indie", "Pop"])
    .agg(F.max("total_debut_year"))
    .orderBy("row_count")
)
