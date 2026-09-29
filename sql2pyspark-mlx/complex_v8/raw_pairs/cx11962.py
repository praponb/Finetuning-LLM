from pyspark.sql import functions as F

cleaned_artists = (
    df_artists
    .filter((F.col("genre") == "HipHop") & (F.col("is_verified")))
    .select(
        "id",
        "artist_name",
        "debut_year",
        "is_verified",
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("country")), " ", "_")).alias("country_slug"),
    )
)

tagged = (
    cleaned_artists
    .withColumn("debut_year_tier", F.when(F.col("debut_year") >= 2015, "HIGH").when(F.col("debut_year") >= 1997, "MEDIUM").otherwise("LOW"))
)

stats = (
    tagged
    .groupBy("is_verified")
    .agg(
        F.max("debut_year").alias("max_debut_year"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
        F.sum(F.when(~F.col("is_verified"), 1).otherwise(0)).alias("n_is_verified_false"),
        F.round(F.stddev("debut_year"), 2).alias("stddev_debut_year"),
    )
)

screened = (
    stats
    .crossJoin(stats.agg(F.avg("cnt").alias("overall_avg_cnt")))
    .filter(F.col("cnt") > F.col("overall_avg_cnt"))
    .drop("overall_avg_cnt")
)

scored = (
    screened
    .filter(F.col("stddev_debut_year").between(101.2, 1517.2))
    .select(
        "is_verified",
        "cnt",
        "artist_name_list",
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
        (F.floor(F.col("max_debut_year") / 5) * 5).alias("max_debut_year_bucket"),
    )
)

screened_2 = (
    scored
    .crossJoin(scored.agg(F.avg("max_debut_year_bucket").alias("overall_avg_max_debut_year_bucket")))
    .filter(F.col("max_debut_year_bucket") > F.col("overall_avg_max_debut_year_bucket"))
    .drop("overall_avg_max_debut_year_bucket")
)

qualified = (
    screened_2
    .crossJoin(screened_2.agg(F.min("is_verified_flag").alias("overall_min_is_verified_flag")))
    .filter(F.col("is_verified_flag") > F.col("overall_min_is_verified_flag"))
    .drop("overall_min_is_verified_flag")
)

result = (
    qualified
    .filter(F.col("cnt") < 9)
    .select(
        "is_verified",
        "cnt",
        "artist_name_list",
        "max_debut_year_bucket",
        F.abs(F.col("max_debut_year_bucket") - 2000).alias("max_debut_year_bucket_distance"),
        F.when(F.col("is_verified_flag") >= 1, "large").when(F.col("is_verified_flag") >= 0, "medium").otherwise("small").alias("is_verified_flag_tier"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
