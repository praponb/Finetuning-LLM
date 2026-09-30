from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter(F.col("device").isin("speaker", "phone"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
        F.initcap(F.trim(F.col("source"))).alias("source_clean_2"),
        F.concat_ws(": ", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

screened = (
    scoped_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (~F.col("p.device").isin("speaker", "desktop")), "left_anti")
)

enriched = (
    screened
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "is_skipped",
        "source_clean_2",
        "device_source_label",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.date_trunc("hour", F.col("played_at")).alias("played_at_hour"),
    )
)

joined_listener = (
    enriched.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select("s.*", F.col("l.country").alias("listener_country"))
)

result = (
    joined_listener
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "played_at",
        "device",
        "is_skipped",
        "source_clean_2",
        "device_source_label",
        "played_at_hour",
        F.concat(F.col("listener_country"), F.lit("#"), F.col("id").cast("string")).alias("listener_country_ref"),
    )
    .orderBy(F.col("played_at").desc(), F.col("source_clean_2").desc())
)
