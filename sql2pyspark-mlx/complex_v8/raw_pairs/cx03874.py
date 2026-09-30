from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter((F.col("source") == "search") & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

enriched_listener = (
    scoped.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.ms_played_filled",
        "s.source_clean",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.plan").alias("listener_plan"),
    )
)

derived = (
    enriched_listener
    .select(
        "*",
        F.coalesce(F.col("listener_birth_year"), F.lit(0)).alias("listener_birth_year_filled"),
        (F.floor(F.col("listener_birth_year") / 10) * 10).alias("listener_birth_year_bucket"),
        (F.col("ms_played_filled") + F.col("ms_played")).alias("ms_played_filled_plus_ms_played"),
    )
)

track_id_stats = (
    derived
    .groupBy("track_id")
    .agg(F.avg("listener_birth_year").alias("avg_listener_birth_year"))
)

above_average = (
    derived.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.listener_birth_year") > F.col("g.avg_listener_birth_year"))
    .select(
        "s.*",
        "g.avg_listener_birth_year",
        F.round(F.col("s.listener_birth_year") - F.col("g.avg_listener_birth_year"), 2).alias("listener_birth_year_minus_avg"),
    )
)

result = (
    above_average
    .groupBy("listener_plan", "track_id")
    .pivot("source", ["search", "playlist", "album", "radio"])
    .agg(F.max("ms_played_filled").alias("max_v"), F.sum("ms_played_filled").alias("sum_v"))
    .orderBy("listener_plan", "track_id")
)
