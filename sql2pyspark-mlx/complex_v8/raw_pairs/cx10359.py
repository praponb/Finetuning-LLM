from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter(F.col("device").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
    )
)

result = (
    filtered_plays
    .rollup("device", "track_id", "is_skipped")
    .agg(F.array_sort(F.collect_set("device")).alias("device_list"))
    .select(
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "track_id",
        "is_skipped",
        "device_list",
    )
    .orderBy(
        F.col("device").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
        F.col("is_skipped").asc_nulls_last(),
    )
)
