from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

result = (
    cleaned_plays
    .groupingSets(
        [["listener_id", "source", "track_id"], ["listener_id"], ["source"]],
        "listener_id", "source", "track_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("ms_played"), 2).alias("stddev_ms_played"),
        F.sum(F.when(~F.col("is_skipped"), 1).otherwise(0)).alias("n_not_is_skipped"),
        F.sum(F.when(F.col("device").isin("speaker", "car", "tv"), F.col("ms_played")).otherwise(0)).alias("ms_played_device_in_speaker_car_tv"),
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("source").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
    )
)
