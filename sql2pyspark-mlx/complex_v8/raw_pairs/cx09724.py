from pyspark.sql import functions as F

filtered = (
    df_artists
    .filter((~F.col("is_verified")) & (F.upper(F.trim(F.col("artist_name"))).like("P%")))
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        (F.floor(F.col("debut_year") / 5) * 5).alias("debut_year_bucket"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.concat(F.col("artist_name"), F.lit("#"), F.col("debut_year_bucket").cast("string")).alias("artist_name_ref"),
    )
)

filtered_2 = (
    enriched.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.duration_sec") > 165) & (F.col("t.release_date").isNotNull()), "left_anti")
)

result = (
    filtered_2
    .filter(F.col("artist_name_ref").isNotNull())
    .select(
        "id",
        "country",
        "debut_year",
        "debut_year_bucket",
        "artist_name_ref",
        F.when(F.col("country").isin("TH", "US"), "group_a").when(F.col("country") == "JP", "group_b").otherwise("other").alias("country_group"),
    )
    .orderBy(F.col("country").desc(), "debut_year")
)
