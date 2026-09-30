from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter((F.col("ms_played") > 56848) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

annotated = (
    filtered_plays
    .filter(F.col("device").isin("desktop", "speaker"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "ms_played_bucket",
        (F.col("ms_played") - F.col("ms_played_bucket")).alias("ms_played_minus_ms_played_bucket"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.when(F.col("source").isin("search", "playlist"), "group_a").when(F.col("source") == "album", "group_b").otherwise("other").alias("source_group"),
    )
)

tagged = (
    annotated
    .select(
        "*",
        F.coalesce(F.col("ms_played_bucket"), F.lit(0)).alias("ms_played_bucket_filled"),
        F.when(F.col("ms_played_bucket") >= 242000, "HIGH").when(F.col("ms_played_bucket") >= 123500, "MEDIUM").otherwise("LOW").alias("ms_played_bucket_tier"),
    )
)

with_activity = (
    tagged.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device") != "tv"), "left_semi")
)

result = (
    with_activity
    .rollup("source_group", "source", "track_id")
    .agg(
        F.count(F.when(F.col("source").isin("search", "playlist"), 1)).alias("n_source_in_search_playlist"),
    )
    .orderBy(
        F.col("source_group").asc_nulls_last(),
        F.col("source").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
    )
)
