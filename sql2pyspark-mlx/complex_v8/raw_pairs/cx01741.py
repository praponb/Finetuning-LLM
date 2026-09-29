from pyspark.sql import functions as F

filtered = (
    df_artists
    .filter((F.col("country").isin("JP", "BR")) | (F.col("is_verified")))
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
        (F.floor(F.col("debut_year") / 5) * 5).alias("debut_year_bucket"),
        F.concat_ws(" / ", F.col("artist_name"), F.col("genre")).alias("artist_name_genre_label"),
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
    )
)

genre_stats = (
    filtered
    .groupBy("genre")
    .agg(F.avg("debut_year").alias("avg_debut_year"))
)

vs_group = (
    filtered.alias("s")
    .join(genre_stats.alias("g"), F.col("g.genre") == F.col("s.genre"), "inner")
    .filter(F.col("s.debut_year") > F.col("g.avg_debut_year"))
    .select(
        "s.*",
        "g.avg_debut_year",
        F.round(F.col("s.debut_year") - F.col("g.avg_debut_year"), 2).alias("debut_year_minus_avg"),
    )
)

enriched = (
    vs_group
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
        "debut_year_minus_avg",
        F.coalesce(F.col("debut_year_bucket"), F.lit(0)).alias("debut_year_bucket_filled"),
        F.lower(F.trim(F.col("artist_name"))).alias("artist_name_clean"),
        F.greatest(F.col("debut_year"), F.col("debut_year_minus_avg")).alias("greatest_debut_year_debut_year_minus_avg"),
    )
)

result = (
    enriched
    .filter(F.col("debut_year_bucket_filled") > 2000)
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
        "debut_year_minus_avg",
        "debut_year_bucket_filled",
    )
)
