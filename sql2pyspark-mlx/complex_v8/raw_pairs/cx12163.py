from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws(" / ", F.col("device"), F.col("source")).alias("device_source_label"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

scored = (
    filtered_plays
    .select(
        "*",
        F.coalesce(F.col("ms_played_bucket"), F.lit(0)).alias("ms_played_bucket_filled"),
        (F.col("ms_played") + F.col("ms_played_bucket")).alias("ms_played_plus_ms_played_bucket"),
    )
)

result = (
    scored
    .cube("track_id", "source", "device")
    .agg(
        F.sum(F.when(F.col("source").isin("radio", "album"), 1).otherwise(0)).alias("n_source_in_radio_album"),
    )
    .orderBy(
        F.col("track_id").asc_nulls_last(),
        F.col("source").asc_nulls_last(),
        F.col("device").asc_nulls_last(),
    )
)
