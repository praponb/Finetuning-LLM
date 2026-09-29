from pyspark.sql import functions as F

valid = (
    df_plays
    .filter(F.col("device") == "speaker")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
        F.abs(F.col("ms_played") - 178909).alias("ms_played_distance"),
        F.concat_ws("-", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

grouped = (
    valid
    .filter(F.col("device").isNotNull())
    .groupBy("device", "source", "track_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("ms_played").alias("max_ms_played"),
        F.count(F.when(F.col("device") == "speaker", 1)).alias("n_device_speaker"),
        F.round(F.stddev("ms_played"), 2).alias("stddev_ms_played"),
        F.round(F.sum("ms_played_distance") / F.nullif(F.sum("ms_played"), F.lit(0)), 3).alias("ms_played_distance_to_ms_played_ratio"),
    )
)

result = (
    grouped
    .groupBy("n_rows")
    .pivot("device", ["speaker", "tv", "car", "desktop"])
    .agg(F.sum("stddev_ms_played"))
    .orderBy("n_rows")
)
