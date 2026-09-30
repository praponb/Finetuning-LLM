from pyspark.sql import functions as F

valid = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.when(F.col("ms_played") >= 291723, "HIGH").when(F.col("ms_played") >= 145720, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

enriched = (
    valid.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.device_len",
        "s.ms_played_tier",
        F.col("l.country").alias("listener_country"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device") != "desktop"), "left_semi")
    .filter(F.col("track_is_explicit"))
)

result = (
    filtered
    .filter(F.col("device_len") > 7)
    .select(
        "id",
        "ms_played",
        "device",
        "ms_played_tier",
        "listener_country",
        "track_is_explicit",
        F.lower(F.regexp_replace(F.trim(F.col("listener_country")), " ", "_")).alias("listener_country_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("listener_country")), " ", "_")).alias("listener_country_slug_2"),
    )
    .orderBy(F.col("listener_country_slug_2").desc(), "device", "id")
    .limit(5)
)
