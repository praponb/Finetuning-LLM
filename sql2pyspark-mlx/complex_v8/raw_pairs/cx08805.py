from pyspark.sql import functions as F

scoped_artists = (
    df_artists
    .filter(F.col("country").isin("TH", "BR"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
        (F.floor(F.col("debut_year") / 5) * 5).alias("debut_year_bucket"),
        F.when(F.col("genre").isin("Jazz", "HipHop"), "group_a").when(F.col("genre") == "Classical", "group_b").otherwise("other").alias("genre_group"),
    )
)

annotated = (
    scoped_artists
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year_bucket",
        "genre_group",
        F.concat(F.col("country"), F.lit("#"), F.col("debut_year").cast("string")).alias("country_ref"),
        F.concat(F.col("country"), F.lit("#"), F.col("debut_year_bucket").cast("string")).alias("country_ref_2"),
        F.when(F.col("debut_year_bucket") >= 2008, "large").when(F.col("debut_year_bucket") >= 2003, "medium").otherwise("small").alias("debut_year_bucket_tier"),
    )
)

result = (
    annotated
    .groupBy("debut_year_bucket_tier", "country")
    .pivot("genre_group", ["group_a", "other", "group_b"])
    .agg(F.sum("debut_year_bucket"))
    .orderBy("debut_year_bucket_tier", "country")
)
