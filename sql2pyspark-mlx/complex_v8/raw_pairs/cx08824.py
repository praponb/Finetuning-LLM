from pyspark.sql import functions as F

valid = (
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

scored = (
    valid
    .filter(F.col("source") == "playlist")
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.when(F.col("source").isin("search", "playlist"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
    )
)

result = (
    scored
    .groupingSets(
        [["listener_id", "is_skipped_label"], ["listener_id"], []],
        "listener_id", "is_skipped_label",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("is_skipped_label").asc_nulls_last(),
    )
)
