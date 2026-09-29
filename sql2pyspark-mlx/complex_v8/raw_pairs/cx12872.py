from pyspark.sql import functions as F

base = (
    df_plays
    .filter((F.col("track_id") >= 17) | (F.col("listener_id") >= 12))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.when(F.col("device").isin("desktop", "speaker"), "group_a").when(F.col("device") == "car", "group_b").otherwise("other").alias("device_group"),
        F.abs(F.col("ms_played") - 245877).alias("ms_played_distance"),
    )
)

stats = (
    base
    .groupBy("source", "listener_id")
    .agg(
        F.round(100 * F.count(F.when(~F.col("source").isin("search", "playlist"), 1)) / F.count("*"), 1).alias("pct_source_not_in_search_playlis"),
        F.sum("ms_played").alias("total_ms_played"),
        F.avg("ms_played_distance").alias("avg_ms_played_distance_check"),
    )
    .filter(F.col("avg_ms_played_distance_check") > 80000)
    .drop("avg_ms_played_distance_check")
)

derived = (
    stats
    .select(
        "source",
        "listener_id",
        "pct_source_not_in_search_playlis",
        "total_ms_played",
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        (F.floor(F.col("total_ms_played") / 1000000) * 1000000).alias("total_ms_played_bucket"),
    )
)

with_details = (
    derived.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.country") != "UK")
    .select(
        "s.*",
        F.col("l.username").alias("listener_username"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.country").alias("listener_country"),
    )
)

with_children_plays = (
    with_details.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.source",
        "s.listener_id",
        "s.total_ms_played",
        "s.source_prefix",
        "s.total_ms_played_bucket",
        "s.listener_country",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
    )
)

listener_country_stats = (
    with_children_plays
    .groupBy("listener_country")
    .agg(F.percentile_approx("total_ms_played", 0.5).alias("median_total_ms_played"))
)

compared = (
    with_children_plays.alias("s")
    .join(listener_country_stats.alias("g"), F.col("g.listener_country") == F.col("s.listener_country"), "inner")
    .filter(F.col("s.total_ms_played") >= F.col("g.median_total_ms_played"))
    .select(
        "s.*",
        "g.median_total_ms_played",
        F.round(F.col("s.total_ms_played") - F.col("g.median_total_ms_played"), 2).alias("total_ms_played_minus_median"),
    )
)

tagged = (
    compared
    .select(
        "source",
        "listener_id",
        "total_ms_played",
        "total_ms_played_bucket",
        "listener_country",
        "play_id",
        "play_is_skipped",
        "play_played_at",
        "play_source",
        "median_total_ms_played",
        (F.col("total_ms_played") + F.col("median_total_ms_played")).alias("total_ms_played_plus_median_total_ms_played"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.abs(F.col("total_ms_played_bucket") - 843250).alias("total_ms_played_bucket_distance"),
    )
)

result = (
    tagged
    .filter(F.col("source_slug").isNotNull())
    .select(
        "source",
        "listener_id",
        "total_ms_played_bucket",
        "listener_country",
        "play_is_skipped",
        "play_played_at",
        "play_source",
        "median_total_ms_played",
        "total_ms_played_bucket_distance",
    )
)
