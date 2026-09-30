from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
    )
)

annotated = (
    prepared_plays
    .select(
        "*",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.abs(F.col("ms_played") - 245877).alias("ms_played_distance"),
    )
)

result = (
    annotated
    .select(
        "id",
        "ms_played",
        "is_skipped_flag",
        "ms_played_distance",
    )
    .unpivot(
        ids=["id"],
        values=["ms_played", "is_skipped_flag", "ms_played_distance"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
