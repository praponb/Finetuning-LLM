from pyspark.sql import functions as F

valid = (
    df_plays
    .filter((F.col("source").isin("search", "album", "radio")) & (F.month(F.col("played_at")).isin(1, 2)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

profiled = (
    valid.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.first_played_at")
)

rollup_base = (
    profiled
    .groupBy(
        "is_skipped",
        F.date_trunc("month", "played_at").alias("played_at_month"),
        F.when(F.col("ms_played") >= 245877, "HIGH").when(F.col("ms_played") >= 145720, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
    )
    .agg(
        F.max("ms_played").alias("max_ms_played"),
        F.min("ms_played").alias("min_ms_played"),
        F.avg("ms_played").alias("avg_ms_played_check"),
    )
    .filter(F.col("avg_ms_played_check") > 92277)
    .drop("avg_ms_played_check")
)

enriched = (
    rollup_base
    .select(
        "is_skipped",
        "played_at_month",
        "ms_played_tier",
        "min_ms_played",
        F.length(F.trim(F.col("ms_played_tier"))).alias("ms_played_tier_len"),
    )
)

result = (
    enriched
    .groupingSets(
        [["ms_played_tier_len", "ms_played_tier"], ["ms_played_tier_len"]],
        "ms_played_tier_len", "ms_played_tier",
    )
    .agg(
        F.sum(F.when(F.col("is_skipped"), 1).otherwise(0)).alias("n_is_skipped_true"),
        F.min("min_ms_played").alias("min_min_ms_played"),
    )
    .select(
        "ms_played_tier_len",
        F.coalesce(F.col("ms_played_tier"), F.lit("ALL")).alias("ms_played_tier"),
        "n_is_skipped_true",
        "min_min_ms_played",
    )
    .orderBy(
        F.col("ms_played_tier_len").asc_nulls_last(),
        F.col("ms_played_tier").asc_nulls_last(),
    )
)
