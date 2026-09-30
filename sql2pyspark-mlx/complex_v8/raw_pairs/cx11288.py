from pyspark.sql import functions as F

prepared = (
    df_artists
    .filter((F.col("debut_year") > 1996) & (F.col("genre") == "HipHop"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        F.substring(F.trim(F.col("genre")), 1, 3).alias("genre_prefix"),
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
        F.substring(F.trim(F.col("country")), 1, 4).alias("country_prefix"),
    )
)

derived = (
    prepared
    .filter(F.col("is_verified_flag") < 1)
    .select(
        "*",
        F.when(F.col("is_verified_flag") >= 1, "A").when(F.col("is_verified_flag") >= 0, "B").otherwise("C").alias("is_verified_flag_tier"),
    )
)

without_matches = (
    derived.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left_semi")
    .filter(~F.col("is_verified_flag_tier").isin("A", "B"))
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
    )
)

with_counts = (
    without_matches.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "track_is_explicit_count",
        "is_verified_flag",
        "debut_year",
    )
    .unpivot(
        ids=["id"],
        values=["track_is_explicit_count", "is_verified_flag", "debut_year"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric_name")
)
