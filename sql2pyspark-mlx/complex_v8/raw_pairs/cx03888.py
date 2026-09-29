from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter(F.col("device").isin("tv", "phone"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.concat_ws(": ", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

scored = (
    scoped_plays
    .select(
        "id",
        "track_id",
        "is_skipped",
        "device_source_label",
        F.when(F.col("source").isin("playlist", "album"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
        F.when(F.col("source").isin("playlist", "album"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group_2"),
    )
)

result = (
    scored
    .rollup("source_group", "track_id", "is_skipped")
    .agg(F.count("*").alias("n_rows"))
    .orderBy(
        F.col("source_group").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
        F.col("is_skipped").asc_nulls_last(),
    )
)
