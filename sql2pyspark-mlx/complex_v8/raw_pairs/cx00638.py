from pyspark.sql import functions as F

cleaned = (
    df_artists
    .filter((F.col("country").isin("US", "JP")) & (F.col("debut_year") > 1997))
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
    )
)

tagged = (
    cleaned
    .filter(F.col("genre") != "HipHop")
    .select(
        "id",
        "genre",
        "debut_year",
        "debut_year_filled",
        F.length(F.trim(F.col("country"))).alias("country_len"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
    )
)

summarized_rows = (
    tagged.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
    )
)

result = (
    summarized_rows
    .groupBy("country_len", "track_is_explicit_count")
    .pivot("genre", ["Pop", "Classical"])
    .agg(F.max("debut_year").alias("max_v"), F.sum("debut_year").alias("sum_v"))
    .orderBy("country_len", "track_is_explicit_count")
)
