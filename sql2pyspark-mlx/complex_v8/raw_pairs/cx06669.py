from pyspark.sql import functions as F

prepared = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

with_details_listener = (
    prepared.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source",
        F.col("l.plan").alias("listener_plan"),
    )
)

scored = (
    with_details_listener
    .filter(F.col("track_id").isNotNull())
    .select(
        "*",
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
        F.concat_ws(": ", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

result = (
    scored
    .select(
        "id",
        "track_id",
        "device",
        "listener_plan",
        "source_clean",
        "source_device_label",
        F.concat_ws(" / ", F.col("listener_plan"), F.col("source_clean")).alias("listener_plan_source_clean_label"),
        F.coalesce(F.col("listener_plan"), F.lit("UNKNOWN")).alias("listener_plan_or_unknown"),
    )
    .orderBy(F.col("device").asc())
)
