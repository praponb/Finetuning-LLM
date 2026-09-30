from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

per_group = (
    recent_plays
    .groupBy("source", "source_len", "is_skipped")
    .agg(
        F.min("ms_played").alias("min_ms_played"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.round(F.avg("source_len"), 2).alias("avg_source_len"),
        F.max("played_at").alias("last_played_at"),
        F.sum(F.when(F.col("is_skipped"), F.col("source_len")).otherwise(0)).alias("source_len_is_skipped_true"),
        F.round(F.sum("source_len") / F.nullif(F.sum("ms_played"), F.lit(0)), 3).alias("source_len_to_ms_played_ratio"),
    )
)

without_matches = (
    per_group
    .crossJoin(per_group.agg(F.avg("source_len_to_ms_played_ratio").alias("overall_avg_source_len_to_ms_played_ratio")))
    .filter(F.col("source_len_to_ms_played_ratio") > F.col("overall_avg_source_len_to_ms_played_ratio"))
    .drop("overall_avg_source_len_to_ms_played_ratio")
)

enriched = (
    without_matches
    .withColumn("source_ref", F.concat(F.col("source"), F.lit("#"), F.col("min_ms_played").cast("string")))
)

enriched_2 = enriched.withColumn("source_len_2", F.length(F.trim(F.col("source"))))

tagged = (
    enriched_2
    .filter(F.col("min_ms_played").between(64250, 281500))
    .withColumn("source_or_unknown", F.coalesce(F.col("source"), F.lit("UNKNOWN")))
)

result = (
    tagged
    .select(
        "source",
        "is_skipped",
        "min_ms_played",
        "avg_source_len",
        "last_played_at",
        "source_len_is_skipped_true",
        "source_ref",
        "source_len_2",
        "source_or_unknown",
        F.to_date(F.col("last_played_at")).alias("last_played_at_date"),
    )
    .orderBy(F.col("last_played_at").desc())
)
