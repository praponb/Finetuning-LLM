from pyspark.sql import functions as F

cleaned = (
    df_artists
    .filter(F.col("country") == "US")
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
        "is_verified",
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.initcap(F.trim(F.col("artist_name"))).alias("artist_name_clean"),
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
    )
)

scored = cleaned.withColumn("artist_name_clean_2", F.initcap(F.trim(F.col("artist_name"))))

result = (
    scored
    .select("id", "debut_year", "debut_year_filled")
    .unpivot(
        ids=["id"],
        values=["debut_year", "debut_year_filled"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
