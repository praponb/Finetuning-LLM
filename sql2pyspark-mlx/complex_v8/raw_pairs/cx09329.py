from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter(F.col("listener_id") <= 7)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

annotated = (
    scoped_plays
    .select(
        "*",
        F.substring(F.trim(F.col("source_or_unknown")), 1, 3).alias("source_or_unknown_prefix"),
        F.when(F.col("source_or_unknown").isin("radio", "UNKNOWN"), "group_a").when(F.col("source_or_unknown") == "search", "group_b").otherwise("other").alias("source_or_unknown_group"),
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
    )
)

result = (
    annotated
    .groupingSets(
        [["listener_id", "source_or_unknown", "device"], ["listener_id"], ["source_or_unknown"], []],
        "listener_id", "source_or_unknown", "device",
    )
    .agg(F.countDistinct("source").alias("distinct_sources"))
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("source_or_unknown").asc_nulls_last(),
        F.col("device").asc_nulls_last(),
    )
)
