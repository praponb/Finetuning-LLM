from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.abs(F.col("ms_played") - 178909).alias("ms_played_distance"),
        F.concat(F.col("source"), F.lit("#"), F.col("ms_played").cast("string")).alias("source_ref"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

labeled = (
    recent_plays
    .withColumn("greatest_ms_played_ms_played_bucket", F.greatest(F.col("ms_played"), F.col("ms_played_bucket")))
)

result = (
    labeled
    .groupingSets(
        [["track_id", "source", "device"], ["track_id"], ["source"]],
        "track_id", "source", "device",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("greatest_ms_played_ms_played_bucket").alias("total_greatest_ms_played_ms_played_bucket"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("track_id").asc_nulls_last(),
        F.col("source").asc_nulls_last(),
        F.col("device").asc_nulls_last(),
    )
)
