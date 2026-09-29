from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
        F.abs(F.col("duration_sec") - 312).alias("duration_sec_distance"),
        F.when(F.col("duration_sec") >= 357, "A").when(F.col("duration_sec") >= 209, "B").otherwise("C").alias("duration_sec_tier"),
    )
)

enriched = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", F.col("a.is_verified").alias("artist_is_verified"))
)

enriched_2 = (
    enriched
    .select(
        "id",
        "release_date",
        "bpm",
        "title_ref",
        "duration_sec_distance",
        "artist_is_verified",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.when(F.col("duration_sec_tier").isin("B", "C"), "group_a").when(F.col("duration_sec_tier") == "A", "group_b").otherwise("other").alias("duration_sec_tier_group"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

result = (
    enriched_2
    .rollup("artist_is_verified", "duration_sec_tier_group")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("row_count"),
        F.sum("duration_sec_bucket").alias("total_duration_sec_bucket"),
        F.max("release_date").alias("last_release_date"),
    )
    .orderBy(
        F.col("artist_is_verified").asc_nulls_last(),
        F.col("duration_sec_tier_group").asc_nulls_last(),
    )
)
