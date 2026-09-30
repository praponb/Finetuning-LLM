from pyspark.sql import functions as F

valid = (
    df_plays
    .filter(F.col("source") == "search")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.when(F.col("source").isin("radio", "album"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
    )
)

with_activity = (
    valid.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.source").isin("radio", "playlist", "search")), "left_anti")
    .filter(F.col("track_id").isNotNull())
)

result = (
    with_activity
    .select(
        "id",
        "listener_id",
        "is_skipped",
        "source",
        "source_group",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown_2"),
    )
    .orderBy(F.col("source_or_unknown").asc_nulls_last(), "source")
)
