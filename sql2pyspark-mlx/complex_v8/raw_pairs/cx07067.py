from pyspark.sql import functions as F

base = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("source").isin("album", "radio")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.abs(F.col("ms_played") - 239955).alias("ms_played_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

source_stats = (
    base
    .groupBy("source")
    .agg(F.max("ms_played").alias("max_ms_played"))
)

outliers = (
    base.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played") == F.col("g.max_ms_played"))
    .select(
        "s.*",
        "g.max_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.max_ms_played"), 2).alias("ms_played_minus_max"),
    )
)

joined_listener = (
    outliers.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.country") == "BR"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.days_since_played_at",
        "s.ms_played_distance",
        "s.max_ms_played",
        F.col("l.country").alias("listener_country"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.min("played_at").alias("first_played_at"),
        F.min("played_at").alias("first_played_at_2"),
    )
)

profiled = (
    joined_listener.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.first_played_at",
        "p.first_played_at_2",
    )
)

result = (
    profiled
    .groupBy("first_played_at_2", "source")
    .pivot("listener_country", ["UK", "TH", "BR", "KR"])
    .agg(F.avg("listener_birth_year"))
    .orderBy("first_played_at_2", "source")
)
