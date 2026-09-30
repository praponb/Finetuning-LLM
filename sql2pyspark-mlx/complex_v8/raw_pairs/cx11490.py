from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(7, 11))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.date_trunc("quarter", F.col("played_at")).alias("played_at_quarter"),
    )
)

stats = (
    recent_plays
    .groupBy(
        F.when(F.col("ms_played") >= 301865, "large").when(F.col("ms_played") >= 99376, "medium").otherwise("small").alias("ms_played_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("source").isin("search", "playlist", "album"), 1).otherwise(0)).alias("n_source_in_search_playlist_al"),
    )
)

tagged = stats.withColumn("ms_played_tier_clean", F.upper(F.trim(F.col("ms_played_tier"))))

result = (
    tagged
    .select(
        "ms_played_tier",
        "cnt",
        "n_source_in_search_playlist_al",
        "ms_played_tier_clean",
        F.lower(F.regexp_replace(F.trim(F.col("ms_played_tier")), " ", "_")).alias("ms_played_tier_slug"),
    )
    .orderBy("ms_played_tier", F.col("cnt").asc())
)
