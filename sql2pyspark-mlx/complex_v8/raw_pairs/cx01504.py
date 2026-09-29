from pyspark.sql import functions as F

cleaned_artists = (
    df_artists
    .filter((F.col("debut_year") > 1997) & (F.col("is_verified")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.concat_ws("|", F.col("country"), F.col("artist_name")).alias("country_artist_name_label"),
        F.when(F.col("country").isin("US", "KR"), "group_a").when(F.col("country") == "TH", "group_b").otherwise("other").alias("country_group"),
        F.when(F.col("debut_year") >= 2016, "large").when(F.col("debut_year") >= 1997, "medium").otherwise("small").alias("debut_year_tier"),
    )
)

scored = (
    cleaned_artists
    .select(
        "id",
        "genre",
        "debut_year",
        "is_verified",
        "debut_year_tier",
        F.lower(F.trim(F.col("country"))).alias("country_clean"),
    )
)

labeled = (
    scored
    .select(
        "*",
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
        F.when(F.col("debut_year") >= 2018, "HIGH").when(F.col("debut_year") >= 1996, "MEDIUM").otherwise("LOW").alias("debut_year_tier_2"),
    )
)

detail_rows = (
    labeled.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.genre",
        "s.debut_year",
        "s.is_verified",
        "s.country_clean",
        "s.debut_year_filled",
        F.col("t.id").alias("track_id"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.title").alias("track_title"),
    )
)

enriched = (
    detail_rows
    .filter(F.size(F.col("track_moods")) >= 1)
    .select(
        "id",
        "debut_year",
        "is_verified",
        "debut_year_filled",
        "track_id",
        "track_moods",
        "track_title",
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled_2"),
    )
)

annotated = (
    enriched
    .select(
        "*",
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
        F.when(F.col("debut_year") >= 2015, "HIGH").when(F.col("debut_year") >= 1996, "MEDIUM").otherwise("LOW").alias("debut_year_tier"),
        F.when(F.col("debut_year") >= 2015, "A").when(F.col("debut_year") >= 1997, "B").otherwise("C").alias("debut_year_tier_2"),
    )
)

result = (
    annotated
    .select(
        "id",
        "is_verified",
        "debut_year_filled",
        "track_id",
        "track_title",
        "debut_year_filled_2",
        "is_verified_flag",
        "debut_year_tier",
        "debut_year_tier_2",
        F.coalesce(F.col("debut_year_tier"), F.lit("UNKNOWN")).alias("debut_year_tier_or_unknown"),
    )
    .orderBy(
        F.col("debut_year_tier_2").desc(),
        F.col("debut_year_filled_2").desc_nulls_first(),
    )
)
