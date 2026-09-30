from pyspark.sql import functions as F

recent_artists = (
    df_artists
    .filter(F.col("country") == "US")
    .select(
        "id",
        "artist_name",
        "debut_year",
        "is_verified",
        F.concat(F.col("genre"), F.lit("#"), F.col("debut_year").cast("string")).alias("genre_ref"),
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
        F.substring(F.trim(F.col("country")), 1, 3).alias("country_prefix"),
    )
)

grouped = (
    recent_artists
    .groupBy("is_verified_flag", "is_verified")
    .agg(
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
        F.max("is_verified_flag").alias("max_is_verified_flag"),
        F.round(F.sum("is_verified_flag") / F.nullif(F.sum("debut_year"), F.lit(0)), 3).alias("is_verified_flag_to_debut_year_ratio"),
        F.round(F.stddev("debut_year"), 2).alias("stddev_debut_year"),
    )
)

derived = (
    grouped
    .select(
        "is_verified_flag",
        "is_verified",
        "max_is_verified_flag",
        "stddev_debut_year",
        F.size(F.col("artist_name_list")).alias("artist_name_list_count"),
    )
)

qualified = (
    derived
    .crossJoin(derived.agg(F.avg("max_is_verified_flag").alias("overall_avg_max_is_verified_flag")))
    .filter(F.col("max_is_verified_flag") > F.col("overall_avg_max_is_verified_flag"))
    .drop("overall_avg_max_is_verified_flag")
)

result = (
    qualified
    .select(
        "is_verified_flag",
        "is_verified",
        "artist_name_list_count",
        "max_is_verified_flag",
    )
    .unpivot(
        ids=["is_verified_flag", "is_verified"],
        values=["artist_name_list_count", "max_is_verified_flag"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("is_verified_flag", "is_verified", "measure")
)
