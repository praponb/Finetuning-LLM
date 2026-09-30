from pyspark.sql import functions as F

valid = (
    df_plays
    .filter(F.col("listener_id") >= 15)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
    )
)

rollup_base = (
    valid
    .filter(F.col("listener_id").isNotNull())
    .groupBy("is_skipped")
    .agg(
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
    )
)

result = (
    rollup_base
    .filter(F.col("distinct_listener_ids") > 2)
    .select("is_skipped", "distinct_listener_ids")
    .orderBy(F.col("distinct_listener_ids").asc())
)
