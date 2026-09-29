from pyspark.sql import functions as F

filtered_artists = (
    df_artists
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
        F.upper(F.trim(F.col("country"))).alias("country_clean"),
    )
)

screened = (
    filtered_artists
    .crossJoin(filtered_artists.agg(F.min("is_verified_flag").alias("overall_min_is_verified_flag")))
    .filter(F.col("is_verified_flag") > F.col("overall_min_is_verified_flag"))
    .drop("overall_min_is_verified_flag")
)

tagged = (
    screened
    .filter(~F.col("genre").isin("Jazz", "Classical"))
    .select(
        "*",
        F.substring(F.trim(F.col("genre")), 1, 2).alias("genre_prefix"),
        F.when(F.col("is_verified_flag") >= 1, "HIGH").when(F.col("is_verified_flag") >= 0, "MEDIUM").otherwise("LOW").alias("is_verified_flag_tier"),
    )
)

exploded_detail_tracks = (
    tagged.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_name",
        "s.country",
        "s.debut_year",
        "s.is_verified_flag",
        F.col("t.id").alias("track_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    exploded_detail_tracks
    .groupBy("is_verified_flag")
    .pivot("country", ["US", "TH", "JP"])
    .agg(F.sum("track_duration_sec"))
    .orderBy("is_verified_flag")
)
