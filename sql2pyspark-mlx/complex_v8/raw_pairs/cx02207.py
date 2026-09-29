from pyspark.sql import functions as F

prepared_artists = (
    df_artists
    .filter(F.length(F.trim(F.col("artist_name"))) >= 4)
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
        "is_verified",
        F.when(F.col("debut_year") >= 2020, "HIGH").when(F.col("debut_year") >= 1997, "MEDIUM").otherwise("LOW").alias("debut_year_tier"),
    )
)

genre_stats = (
    prepared_artists
    .groupBy("genre")
    .agg(F.avg("debut_year").alias("avg_debut_year"))
)

compared = (
    prepared_artists.alias("s")
    .join(genre_stats.alias("g"), F.col("g.genre") == F.col("s.genre"), "inner")
    .filter(F.col("s.debut_year") > F.col("g.avg_debut_year"))
    .select(
        "s.*",
        "g.avg_debut_year",
        F.round(F.col("s.debut_year") - F.col("g.avg_debut_year"), 2).alias("debut_year_minus_avg"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.max("release_date").alias("last_track_release_date"))
)

with_counts = (
    compared.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_track_release_date")
)

scored = (
    with_counts
    .filter(F.col("debut_year_tier").isin("HIGH", "LOW"))
    .select(
        "*",
        F.date_trunc("week", F.col("last_track_release_date")).alias("last_track_release_date_week"),
        F.concat(F.col("debut_year_tier"), F.lit("#"), F.col("id").cast("string")).alias("debut_year_tier_ref"),
    )
)

derived = (
    scored
    .filter(F.col("debut_year_tier_ref").isNotNull())
    .withColumn("genre_slug", F.lower(F.regexp_replace(F.trim(F.col("genre")), " ", "_")))
)

track_stats_2 = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count_2"),
        F.count(F.when(F.col("is_explicit"), 1)).alias("track_is_explicit_count_3"),
    )
)

with_stats = (
    derived.alias("s")
    .join(track_stats_2.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
        F.coalesce(F.col("t.track_is_explicit_count_2"), F.lit(0)).alias("track_is_explicit_count_2"),
        F.coalesce(F.col("t.track_is_explicit_count_3"), F.lit(0)).alias("track_is_explicit_count_3"),
    )
)

track_stats_3 = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.round(F.avg("bpm"), 2).alias("avg_track_bpm"),
    )
)

with_stats_2 = (
    with_stats.alias("s")
    .join(track_stats_3.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.avg_track_bpm",
    )
)

result = (
    with_stats_2
    .filter(F.year(F.col("last_track_release_date")) == 2024)
    .select(
        "id",
        "artist_name",
        "is_verified",
        "debut_year_tier",
        "avg_debut_year",
        "debut_year_tier_ref",
        "genre_slug",
        "track_is_explicit_count",
        "track_is_explicit_count_3",
        "track_count",
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
    )
    .orderBy(F.col("track_count").desc(), "debut_year_tier")
)
