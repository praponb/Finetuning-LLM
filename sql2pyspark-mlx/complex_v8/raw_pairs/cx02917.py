from pyspark.sql import functions as F

base = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
    )
)

joined = (
    base.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        F.col("l.country").alias("listener_country"),
    )
)

tagged = (
    joined
    .filter(F.col("listener_country") != "BR")
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.coalesce(F.col("listener_country"), F.lit("UNKNOWN")).alias("listener_country_or_unknown"),
    )
)

result = (
    tagged
    .select(
        "id",
        "device",
        "is_skipped",
        "listener_country",
        "device_slug",
        "device_len",
        "listener_country_or_unknown",
    )
    .orderBy(F.col("id").asc_nulls_last(), "device")
    .limit(5)
)
